package org.cron.distributed_cron_scheduler.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.cron.distributed_cron_scheduler.messaging.JobTaskMessage;
import org.cron.distributed_cron_scheduler.messaging.RabbitMQConfig;
import org.cron.distributed_cron_scheduler.repository.JobDefinitionRepository;
import org.cron.distributed_cron_scheduler.service.CronService;
import org.cron.distributed_cron_scheduler.service.RedisScheduleService;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The Scheduler (Manager) — polls the Redis ZSET every second and dispatches
 * due jobs to RabbitMQ.
 *
 * <h3>Execution flow per poll cycle</h3>
 * <ol>
 *   <li>Call {@code ZPOPMIN cron:schedule 0 &lt;nowMs&gt;} via a Lua script —
 *       atomically removes and returns all job IDs whose score ≤ now.</li>
 *   <li>For each due job ID, fetch the {@link JobDefinition} from PostgreSQL.</li>
 *   <li>Compute the next execution instant from the cron expression (IST-aware).</li>
 *   <li>Re-insert the job into the ZSET with the new score (next run time).</li>
 *   <li>Update {@code next_execution_time} in PostgreSQL (used as fallback for ZSET rebuild on restart).</li>
 *   <li>Build a {@link JobTaskMessage} for each due job.</li>
 *   <li>Batch-update all modified jobs in Postgres in one {@code saveAll()}.</li>
 *   <li>Publish all messages to RabbitMQ in parallel using NON_PERSISTENT delivery.</li>
 * </ol>
 *
 * <h3>Concurrency safety</h3>
 * The Redis Lua script is atomic on the server side — even if multiple Scheduler
 * pods run concurrently, each job ID is popped and returned by exactly one pod.
 * No database-level locking is needed.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SchedulerService {

    private final RedisScheduleService    redisScheduleService;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final CronService             cronService;
    private final RabbitTemplate          rabbitTemplate;

    @Value("${scheduler.poll-interval-ms:1000}")
    private long pollIntervalMs;

    // ── Polling loop ─────────────────────────────────────────────────────────

    /**
     * Main polling loop. Runs at a fixed rate (wall-clock aligned), defaulting to every 1 second.
     *
     * <p>Uses {@code fixedRate} (not {@code fixedDelay}) so the next invocation is always
     * scheduled relative to the previous <em>start</em> time, not its completion.
     * This keeps the polling interval wall-clock aligned.
     */
    @Scheduled(fixedRateString = "${scheduler.poll-interval-ms:1000}")
    @Transactional
    public void pollAndDispatch() {
        Instant now        = Instant.now();
        long    nowEpochMs = now.toEpochMilli();

        // ── 1. Atomically pop all due job IDs from Redis ZSET ────────────────
        Set<String> dueJobIds = redisScheduleService.popDueJobs(nowEpochMs);

        if (dueJobIds.isEmpty()) {
            log.trace("No due jobs in Redis ZSET at {}", now);
            return;
        }

        log.info("Scheduler popped {} due job(s) from Redis ZSET at {}", dueJobIds.size(), now);

        List<JobTaskMessage> messages   = new ArrayList<>();
        List<JobDefinition>  toSave     = new ArrayList<>();

        for (String jobId : dueJobIds) {
            try {
                // ── 2. Fetch job definition from Postgres ────────────────────
                JobDefinition job = jobDefinitionRepository.findById(UUID.fromString(jobId))
                        .orElse(null);

                if (job == null) {
                    log.warn("Job id={} was in Redis ZSET but not found in Postgres — removing from ZSET", jobId);
                    // Don't re-add to ZSET; it was probably deleted
                    continue;
                }

                if (!Boolean.TRUE.equals(job.getEnabled())) {
                    log.warn("Job '{}' (id={}) is disabled — removing from ZSET", job.getName(), jobId);
                    redisScheduleService.removeJob(jobId);
                    continue;
                }

                // ── 3. Compute next execution slot from cron expression ──────
                Instant nextRun = cronService.nextExecution(job.getCronExpression(), now);

                // ── 4. Re-insert into Redis ZSET with new score ──────────────
                redisScheduleService.scheduleJob(jobId, nextRun.toEpochMilli());

                // ── 5. Update Postgres next_execution_time (fallback cache) ──
                job.setNextExecutionTime(nextRun);
                toSave.add(job);

                // ── 6. Build RabbitMQ message ────────────────────────────────
                messages.add(JobTaskMessage.builder()
                        .jobId(job.getId())
                        .jobName(job.getName())
                        .targetUrl(job.getTargetUrl())
                        .httpMethod(job.getHttpMethod())
                        .scheduledTime(now)
                        .build());

                log.debug("Prepared dispatch for job '{}' (id={}) — next run at {} IST",
                        job.getName(), job.getId(), nextRun);

            } catch (Exception ex) {
                log.error("Failed to process due job id={}: {}", jobId, ex.getMessage(), ex);
                // Re-add to ZSET so it will be retried on the next cycle
                redisScheduleService.scheduleJob(jobId, nowEpochMs);
            }
        }

        // ── 7. Batch-save next_execution_time updates to Postgres ────────────
        if (!toSave.isEmpty()) {
            jobDefinitionRepository.saveAll(toSave);
        }

        // ── 8. Publish all messages to RabbitMQ in parallel ──────────────────
        messages.parallelStream().forEach(message -> {
            try {
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.EXCHANGE_NAME,
                        RabbitMQConfig.ROUTING_KEY,
                        message,
                        m -> {
                            m.getMessageProperties().setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
                            return m;
                        }
                );
                log.info("Published job '{}' (id={}) to broker", message.getJobName(), message.getJobId());
            } catch (Exception ex) {
                log.error("BROKER PUBLISH FAILED for job '{}' (id={}): {}",
                        message.getJobName(), message.getJobId(), ex.getMessage(), ex);
            }
        });

        log.info("Batch dispatch of {} job(s) completed in {} ms",
                messages.size(), Duration.between(now, Instant.now()).toMillis());
    }
}
