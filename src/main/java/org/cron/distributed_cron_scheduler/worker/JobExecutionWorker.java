package org.cron.distributed_cron_scheduler.worker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.cron.distributed_cron_scheduler.domain.JobExecutionHistory;
import org.cron.distributed_cron_scheduler.enums.ExecutionStatus;
import org.cron.distributed_cron_scheduler.messaging.JobTaskMessage;
import org.cron.distributed_cron_scheduler.messaging.RabbitMQConfig;
import org.cron.distributed_cron_scheduler.repository.JobDefinitionRepository;
import org.cron.distributed_cron_scheduler.repository.JobExecutionHistoryRepository;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.http.HttpMethod;

import java.time.Duration;
import java.time.Instant;

/**
 * The Execution Engine (Worker) — consumes {@link JobTaskMessage} objects from RabbitMQ
 * and executes the corresponding HTTP call against the target URL.
 *
 * <h3>Execution flow</h3>
 * <ol>
 *   <li>Message arrives from the {@code cron.jobs} queue.</li>
 *   <li>{@link #actualStartTime} is captured immediately — this is used to compute
 *       scheduling drift together with {@link JobTaskMessage#getScheduledTime()}.</li>
 *   <li>A non-blocking {@link WebClient} call is issued with a configurable response
 *       timeout ({@code scheduler.http-timeout-seconds}).</li>
 *   <li>Regardless of outcome (SUCCESS / FAILED / TIMEOUT), an immutable
 *       {@link JobExecutionHistory} record is written to the database.</li>
 * </ol>
 *
 * <h3>Robustness</h3>
 * <ul>
 *   <li>Network errors ({@link WebClientRequestException}) → status {@code FAILED}</li>
 *   <li>Timeout ({@link java.util.concurrent.TimeoutException}) → status {@code TIMEOUT}</li>
 *   <li>Non-2xx HTTP responses → status {@code FAILED} (HTTP status code is still recorded)</li>
 *   <li>Any unexpected exception → status {@code FAILED}, full message in {@code response_payload}</li>
 * </ul>
 *
 * <h3>Horizontal scaling</h3>
 * Multiple Worker instances may listen on the same queue simultaneously.
 * RabbitMQ distributes messages in round-robin across all connected consumers.
 * Concurrency and prefetch are tuned via {@code spring.rabbitmq.listener.simple.*}
 * in {@code application.yaml}.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JobExecutionWorker {

    private static final int MAX_PAYLOAD_LENGTH = 10_000; // 10 KB cap for stored response body

    private final JobDefinitionRepository    jobDefinitionRepository;
    private final JobExecutionHistoryRepository historyRepository;
    private final WebClient                  webClient;

    @Value("${scheduler.http-timeout-seconds:30}")
    private long httpTimeoutSeconds;

    /**
     * Main consumer method — invoked by Spring AMQP whenever a message arrives
     * on the {@code cron.jobs} queue.
     *
     * @param message the task dispatched by the Scheduler containing target URL,
     *                HTTP method, and scheduled timestamp
     */
    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    public void executeJob(JobTaskMessage message) {

        // Capture the actual start time immediately for accurate drift measurement
        Instant actualStartTime = Instant.now();

        log.info("Worker received job '{}' (id={}): scheduled={}, actual={}",
                message.getJobName(), message.getJobId(),
                message.getScheduledTime(), actualStartTime);

        // Look up the parent entity (needed for the FK in job_execution_history)
        JobDefinition jobDef = jobDefinitionRepository.findById(message.getJobId()).orElse(null);
        if (jobDef == null) {
            log.warn("Job {} no longer exists in DB — skipping execution (was it deleted?)",
                    message.getJobId());
            return;   // ACK the message; no point in requeing a deleted job
        }

        // ── Execute the HTTP call and classify the outcome ───────────────────
        ExecutionStatus status;
        Integer         httpStatusCode  = null;
        String          responsePayload;

        try {
            ResponseEntity<String> response = webClient
                    .method(HttpMethod.valueOf(message.getHttpMethod()))
                    .uri(message.getTargetUrl())
                    // exchangeToMono: does NOT throw for 4xx/5xx — gives us full response
                    .exchangeToMono(clientResponse -> clientResponse.toEntity(String.class))
                    .timeout(Duration.ofSeconds(httpTimeoutSeconds))
                    .block();

            if (response != null) {
                httpStatusCode  = response.getStatusCode().value();
                responsePayload = truncate(response.getBody());
                status = response.getStatusCode().is2xxSuccessful()
                        ? ExecutionStatus.SUCCESS
                        : ExecutionStatus.FAILED;

                log.info("Job '{}' completed: HTTP {}, status={}",
                        message.getJobName(), httpStatusCode, status);
            } else {
                // Shouldn't happen with exchangeToMono, but guard defensively
                status          = ExecutionStatus.FAILED;
                responsePayload = "No response received";
            }

        } catch (WebClientRequestException ex) {
            // Connection refused, DNS failure, etc.
            log.error("Job '{}' (id={}) — network error: {}",
                    message.getJobName(), message.getJobId(), ex.getMessage());
            status          = ExecutionStatus.FAILED;
            responsePayload = "Network error: " + ex.getMessage();

        } catch (Exception ex) {
            // Covers Reactor timeout (TimeoutException wrapped in a RuntimeException),
            // and any other unexpected failures
            if (isTimeout(ex)) {
                log.warn("Job '{}' (id={}) — timed out after {}s",
                        message.getJobName(), message.getJobId(), httpTimeoutSeconds);
                status          = ExecutionStatus.TIMEOUT;
                responsePayload = "Request timed out after " + httpTimeoutSeconds + " seconds";
            } else {
                log.error("Job '{}' (id={}) — unexpected error: {}",
                        message.getJobName(), message.getJobId(), ex.getMessage(), ex);
                status          = ExecutionStatus.FAILED;
                responsePayload = truncate("Unexpected error: " + ex.getMessage());
            }
        }

        // ── Persist the execution record ─────────────────────────────────────
        persistHistory(jobDef, message.getScheduledTime(), actualStartTime,
                status, httpStatusCode, responsePayload);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Writes an immutable {@link JobExecutionHistory} row.
     * The {@code delay_ms} column is computed by PostgreSQL; we only supply raw timestamps.
     */
    private void persistHistory(JobDefinition job,
                                Instant scheduledTime,
                                Instant actualStartTime,
                                ExecutionStatus status,
                                Integer httpStatusCode,
                                String responsePayload) {
        try {
            JobExecutionHistory history = JobExecutionHistory.builder()
                    .job(job)
                    .scheduledTime(scheduledTime)
                    .actualStartTime(actualStartTime)
                    .status(status)
                    .httpStatusCode(httpStatusCode)
                    .responsePayload(responsePayload)
                    .build();

            historyRepository.save(history);

            log.debug("Execution history recorded for job '{}': status={}, delay={}ms",
                    job.getName(), status,
                    Duration.between(scheduledTime, actualStartTime).toMillis());

        } catch (Exception ex) {
            // History persistence failure must not cause message requeue
            // (which would trigger re-execution of the HTTP call)
            log.error("CRITICAL: failed to persist execution history for job '{}' (id={}): {}",
                    job.getName(), job.getId(), ex.getMessage(), ex);
        }
    }

    /**
     * Checks whether an exception (or its cause chain) is a timeout.
     * Reactor wraps {@link java.util.concurrent.TimeoutException} in a RuntimeException
     * when using {@code .block()}.
     */
    private boolean isTimeout(Exception ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof java.util.concurrent.TimeoutException
                    || cause.getClass().getSimpleName().contains("TimeoutException")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Truncates the response payload to {@link #MAX_PAYLOAD_LENGTH} characters
     * to prevent oversized rows in the history table.
     */
    private String truncate(String value) {
        if (value == null) return null;
        return value.length() > MAX_PAYLOAD_LENGTH
                ? value.substring(0, MAX_PAYLOAD_LENGTH) + "...[truncated]"
                : value;
    }
}
