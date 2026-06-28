package org.cron.distributed_cron_scheduler.worker;

import com.rabbitmq.client.Channel;
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
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

/**
 * The Execution Engine (Worker) — consumes {@link JobTaskMessage} objects from RabbitMQ
 * and executes the corresponding HTTP call against the target URL.
 *
 * <h3>Execution flow (Reactive)</h3>
 * <ol>
 *   <li>Message arrives from the {@code cron.jobs} queue along with its delivery tag.</li>
 *   <li>{@link #actualStartTime} is captured immediately for accurate drift measurement.</li>
 *   <li>A non-blocking {@link WebClient} call is initiated. The method returns immediately,
 *       freeing the RabbitMQ listener thread to pick up more messages.</li>
 *   <li>When the HTTP call completes (success, timeout, or error), the pipeline switches
 *       to a bounded elastic thread pool to execute the blocking JPA database write.</li>
 *   <li>Finally, the message is manually acknowledged to RabbitMQ.</li>
 * </ol>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JobExecutionWorker {

    private static final int MAX_PAYLOAD_LENGTH = 10_000; // 10 KB cap for stored response body

    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobExecutionHistoryRepository historyRepository;
    private final WebClient webClient;

    @Value("${scheduler.http-timeout-seconds:30}")
    private long httpTimeoutSeconds;

    /**
     * Main consumer method — invoked by Spring AMQP whenever a message arrives
     * on the {@code cron.jobs} queue. Uses manual acknowledgement.
     */
    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    public void executeJob(JobTaskMessage message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        Instant workerStartTime = Instant.now();

        log.info("Worker received job '{}' (id={}): scheduled={}, worker_start={}",
                message.getJobName(), message.getJobId(),
                message.getScheduledTime(), workerStartTime);

        // 1. Offload DB read (blocking) to elastic scheduler
        Mono.fromCallable(() -> jobDefinitionRepository.findById(message.getJobId()).orElse(null))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(jobDef -> {
                    if (jobDef == null) {
                        log.warn("Job {} no longer exists in DB — skipping execution", message.getJobId());
                        return Mono.empty(); // Still acks the message
                    }

                    // Capture the exact moment the API call begins
                    Instant apiStartTime = Instant.now();

                    // 2. Non-blocking HTTP call on Netty threads
                    return webClient.method(HttpMethod.valueOf(message.getHttpMethod()))
                            .uri(message.getTargetUrl())
                            .exchangeToMono(clientResponse -> clientResponse.toEntity(String.class))
                            .timeout(Duration.ofSeconds(httpTimeoutSeconds))
                            .map(response -> buildResult(response, null, apiStartTime, message))
                            .onErrorResume(ex -> Mono.just(buildResult(null, ex, apiStartTime, message)))
                            // 3. Offload DB write (blocking) back to elastic scheduler
                            .flatMap(result -> persistHistoryReactive(jobDef, message.getScheduledTime(), apiStartTime, result));
                })
                // 4. Manually ACK the message regardless of success or failure in the pipeline
                .doFinally(signalType -> ackMessage(channel, tag, message.getJobId()))
                .subscribe();
    }

    private ExecutionResult buildResult(ResponseEntity<String> response, Throwable ex, Instant actualStartTime, JobTaskMessage message) {
        log.info("API response completed in {} ms", Duration.between(actualStartTime, Instant.now()).toMillis());

        if (ex != null) {
            if (ex instanceof WebClientRequestException) {
                log.error("Job '{}' (id={}) — network error: {}", message.getJobName(), message.getJobId(), ex.getMessage());
                return new ExecutionResult(ExecutionStatus.FAILED, null, "Network error: " + ex.getMessage());
            } else if (isTimeout(ex)) {
                log.warn("Job '{}' (id={}) — timed out after {}s", message.getJobName(), message.getJobId(), httpTimeoutSeconds);
                return new ExecutionResult(ExecutionStatus.TIMEOUT, null, "Request timed out after " + httpTimeoutSeconds + " seconds");
            } else {
                log.error("Job '{}' (id={}) — unexpected error: {}", message.getJobName(), message.getJobId(), ex.getMessage(), ex);
                return new ExecutionResult(ExecutionStatus.FAILED, null, truncate("Unexpected error: " + ex.getMessage()));
            }
        }

        if (response != null) {
            int httpStatusCode = response.getStatusCode().value();
            String responsePayload = truncate(response.getBody());
            ExecutionStatus status = response.getStatusCode().is2xxSuccessful() ? ExecutionStatus.SUCCESS : ExecutionStatus.FAILED;
            log.info("Job '{}' completed: HTTP {}, status={}", message.getJobName(), httpStatusCode, status);
            return new ExecutionResult(status, httpStatusCode, responsePayload);
        }

        return new ExecutionResult(ExecutionStatus.FAILED, null, "No response received");
    }

    private Mono<Void> persistHistoryReactive(JobDefinition job, Instant scheduledTime, Instant actualStartTime, ExecutionResult result) {
        return Mono.fromRunnable(() -> {
            try {
                JobExecutionHistory history = JobExecutionHistory.builder()
                        .job(job)
                        .scheduledTime(scheduledTime)
                        .actualStartTime(actualStartTime)
                        .status(result.status())
                        .httpStatusCode(result.httpStatusCode())
                        .responsePayload(result.responsePayload())
                        .build();

                historyRepository.save(history);
                log.debug("Execution history recorded for job '{}': status={}, delay={}ms",
                        job.getName(), result.status(), Duration.between(scheduledTime, actualStartTime).toMillis());
            } catch (Exception ex) {
                log.error("CRITICAL: failed to persist execution history for job '{}' (id={}): {}",
                        job.getName(), job.getId(), ex.getMessage(), ex);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private void ackMessage(Channel channel, long tag, java.util.UUID jobId) {
        try {
            channel.basicAck(tag, false);
            log.debug("Acknowledged message for job {}", jobId);
        } catch (IOException e) {
            log.error("Failed to acknowledge message for job {}", jobId, e);
        }
    }

    private boolean isTimeout(Throwable ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof TimeoutException || cause.getClass().getSimpleName().contains("TimeoutException")) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String truncate(String value) {
        if (value == null) return null;
        return value.length() > MAX_PAYLOAD_LENGTH
                ? value.substring(0, MAX_PAYLOAD_LENGTH) + "...[truncated]"
                : value;
    }

    private record ExecutionResult(ExecutionStatus status, Integer httpStatusCode, String responsePayload) {}
}
