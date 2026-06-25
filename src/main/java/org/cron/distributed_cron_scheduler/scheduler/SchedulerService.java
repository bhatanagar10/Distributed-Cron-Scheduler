package org.cron.distributed_cron_scheduler.scheduler;

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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The Scheduler (Manager) — polls the Redis ZSET every second and dispatches
 * due jobs to RabbitMQ.
 *
 * <h3>Execution flow per poll cycle</h3>
 * <ol>
 *   <li>Call {@code ZPOPMIN cron:schedule 0 &lt;nowMs&gt;} via a Lua script —
 *       atomically removes and returns all job IDs whose score ≤ now.</li>
 *   <li><b>[OPTIMIZED]</b> Batch-fetch all due {@link JobDefinition}s from PostgreSQL
 *       in a <em>single</em> {@code SELECT … WHERE id IN (…)} query instead of
 *       one query per job (eliminates the N+1 pattern).</li>
 *   <li>Compute the next execution instant from the cron expression (IST-aware).</li>
 *   <li>Re-insert each job into the ZSET with the new score (next run time).</li>
 *   <li>Build a {@link JobTaskMessage} for each due job.</li>
 *   <li><b>[OPTIMIZED]</b> Publish all messages to RabbitMQ in parallel using
 *       NON_PERSISTENT delivery — <em>before</em> writing to PostgreSQL, so workers
 *       start immediately without waiting for the DB write.</li>
 *   <li><b>[OPTIMIZED]</b> Offload the {@code next_execution_time} batch-update to a
 *       background daemon thread pool so it never blocks the dispatch thread.</li>
 * </ol>
 *
 * <h3>Concurrency safety</h3>
 * The Redis Lua script is atomic on the server side — even if multiple Scheduler
 * pods run concurrently, each job ID is popped and returned by exactly one pod.
 * No database-level locking is needed.
 *
 * <h3>Async write safety</h3>
 * {@code next_execution_time} in PostgreSQL is only a <em>fallback cache</em> used to
 * rebuild the Redis ZSET on startup. It does not need to be committed before workers
 * execute. A small lag between the Redis ZSET update and the Postgres write is
 * completely safe.
 */
@Component
@Slf4j
public class SchedulerService {

    private final RedisScheduleService    redisScheduleService;
    private final JobDefinitionRepository jobDefinitionRepository;
    private final CronService             cronService;
    private final RabbitTemplate          rabbitTemplate;

    /**
     * Single background daemon thread for async PostgreSQL writes.
     * Daemon threads are stopped automatically when the JVM exits, so no
     * explicit shutdown hook is needed for this low-criticality write path.
     */
    private final ExecutorService dbWriteExecutor =
            Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "scheduler-db-writer");
                t.setDaemon(true);
                return t;
            });

    @Value("${scheduler.poll-interval-ms:1000}")
    private long pollIntervalMs;

    public SchedulerService(RedisScheduleService redisScheduleService,
                            JobDefinitionRepository jobDefinitionRepository,
                            CronService cronService,
                            RabbitTemplate rabbitTemplate) {
        this.redisScheduleService    = redisScheduleService;
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.cronService             = cronService;
        this.rabbitTemplate          = rabbitTemplate;
    }

    // ── Polling loop ─────────────────────────────────────────────────────────

    /**
     * Main polling loop. Runs at a fixed rate (wall-clock aligned), defaulting to every 1 second.
     *
     * <p>Uses {@code fixedRate} (not {@code fixedDelay}) so the next invocation is always
     * scheduled relative to the previous <em>start</em> time, not its completion.
     * This keeps the polling interval wall-clock aligned.
     *
     * <p><b>Not transactional</b> — no Postgres lock is held while publishing to RabbitMQ.
     * The async DB write runs in its own transaction inside {@link #persistNextExecutionTimes}.
     */
    @Scheduled(fixedRateString = "${scheduler.poll-interval-ms:1000}")
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

        // ── 2. BATCH-FETCH all due job definitions in a single DB round-trip ─
        List<UUID> dueUuids = dueJobIds.stream()
                .map(UUID::fromString)
                .toList();

        Map<UUID, JobDefinition> jobMap = jobDefinitionRepository.findAllById(dueUuids)
                .stream()
                .collect(Collectors.toMap(JobDefinition::getId, Function.identity()));

        List<JobTaskMessage> messages = new ArrayList<>();
        List<JobDefinition>  toSave   = new ArrayList<>();

        for (String jobIdStr : dueJobIds) {
            UUID jobId = UUID.fromString(jobIdStr);
            try {
                JobDefinition job = jobMap.get(jobId);

                if (job == null) {
                    log.warn("Job id={} was in Redis ZSET but not found in Postgres — removing from ZSET", jobIdStr);
                    // Don't re-add to ZSET; job was probably deleted
                    continue;
                }

                if (!Boolean.TRUE.equals(job.getEnabled())) {
                    log.warn("Job '{}' (id={}) is disabled — removing from ZSET", job.getName(), jobIdStr);
                    redisScheduleService.removeJob(jobIdStr);
                    continue;
                }

                // ── 3. Capture true scheduled time BEFORE computing next run ─
                // job.getNextExecutionTime() holds the intended fire time for this cycle.
                // Using Instant.now() here would mask any queue or scheduler delay.
                Instant scheduledTime = job.getNextExecutionTime();

                // ── 4. Compute next execution slot from cron expression ───────
                Instant nextRun = cronService.nextExecution(job.getCronExpression(), now);

                // ── 5. Re-insert into Redis ZSET with new score ───────────────
                redisScheduleService.scheduleJob(jobIdStr, nextRun.toEpochMilli());

                // ── 6. Stage Postgres fallback-cache update (written async) ───
                job.setNextExecutionTime(nextRun);
                toSave.add(job);

                // ── 7. Build RabbitMQ message ─────────────────────────────────
                messages.add(JobTaskMessage.builder()
                        .jobId(job.getId())
                        .jobName(job.getName())
                        .targetUrl(job.getTargetUrl())
                        .httpMethod(job.getHttpMethod())
                        .scheduledTime(scheduledTime)   // true intended fire time
                        .build());

                log.debug("Prepared dispatch for job '{}' (id={}) — next run at {} IST",
                        job.getName(), job.getId(), nextRun);

            } catch (Exception ex) {
                log.error("Failed to process due job id={}: {}", jobIdStr, ex.getMessage(), ex);
                // Re-add to ZSET so it will be retried on the next cycle
                redisScheduleService.scheduleJob(jobIdStr, nowEpochMs);
            }
        }

        // ── 8. Publish all messages to RabbitMQ in parallel (BEFORE DB write) ─
        // Workers receive their tasks immediately; DB write latency is zero-impact.
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

        // ── 9. ASYNC: persist next_execution_time updates in background ───────
        // This is a low-priority fallback-cache write. It must not block dispatch.
        if (!toSave.isEmpty()) {
            final List<JobDefinition> snapshot = List.copyOf(toSave);
            dbWriteExecutor.submit(() -> persistNextExecutionTimes(snapshot));
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Persists the {@code next_execution_time} fallback-cache update for all jobs
     * that were dispatched in this poll cycle.
     *
     * <p>Runs on the dedicated {@link #dbWriteExecutor} daemon thread — completely
     * decoupled from the scheduling hot path. Failures here are non-fatal: the Redis
     * ZSET already holds the authoritative next-run time; Postgres is only queried
     * on a cold restart to rebuild the ZSET.
     *
     * @param jobs the batch of {@link JobDefinition}s to persist
     */
    private void persistNextExecutionTimes(List<JobDefinition> jobs) {
        try {
            jobDefinitionRepository.saveAll(jobs);
            log.debug("Async DB write: persisted next_execution_time for {} job(s)", jobs.size());
        } catch (Exception ex) {
            log.error("Async DB write failed for {} job(s) — next_execution_time may be stale in Postgres: {}",
                    jobs.size(), ex.getMessage(), ex);
        }
    }
}
