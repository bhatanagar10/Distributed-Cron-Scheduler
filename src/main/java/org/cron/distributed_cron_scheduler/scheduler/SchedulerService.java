package org.cron.distributed_cron_scheduler.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.cron.distributed_cron_scheduler.messaging.JobTaskMessage;
import org.cron.distributed_cron_scheduler.messaging.RabbitMQConfig;
import org.cron.distributed_cron_scheduler.repository.JobDefinitionRepository;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * The Scheduler (Manager) — the "clock" component of the distributed cron system.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>An {@link Scheduled @Scheduled} poller fires every {@code scheduler.poll-interval-ms}
 *       milliseconds (default: 5 000 ms — since minimum job interval is 1 minute, polling
 *       every 5 s is more than sufficient and reduces DB load).</li>
 *   <li>Inside a single {@link Transactional @Transactional} boundary it runs:
 *       {@code SELECT ... FOR UPDATE SKIP LOCKED} — acquiring exclusive row locks on all
 *       due jobs, skipping any rows already locked by a peer Scheduler instance.</li>
 *   <li>For each due job:
 *       <ul>
 *         <li>Calculates the <em>next</em> slot using a <strong>drift-resistant</strong>
 *             ceil-based formula anchored to the job's fixed start time (see below).</li>
 *         <li>Registers an {@link TransactionSynchronization#afterCommit()} hook that
 *             publishes the {@link JobTaskMessage} to RabbitMQ <em>only after the
 *             database transaction commits successfully</em>.</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>Drift-resistant next-slot formula</h3>
 * <pre>
 *   anchor      = today (IST) at startHour:startMinute:00
 *   elapsedMs   = now − anchor   (may be negative if anchor is in the future today)
 *   slots       = ceil(elapsedMs / intervalMs)
 *   nextRun     = anchor + slots × intervalMs
 * </pre>
 * This guarantees that even if a job runs late (broker lag, GC pause, etc.) the next
 * slot is always the next clean multiple of {@code interval} past the anchor — never
 * offset by the actual (possibly delayed) execution time.
 *
 * <h3>Transactional safety</h3>
 * Publishing after commit (via {@code afterCommit()}) ensures:
 * <ul>
 *   <li>If the DB commit fails → no message is ever sent (no phantom execution).</li>
 *   <li>If the message send fails after commit → the job's {@code next_execution_time}
 *       has already advanced, so it will fire again at the next interval (at-most-once
 *       semantics for the current window, guaranteed delivery in the next window).</li>
 * </ul>
 *
 * <h3>Horizontal scaling</h3>
 * Multiple Scheduler instances may run concurrently — {@code SKIP LOCKED} guarantees
 * each due job is dispatched by exactly one instance per polling cycle.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SchedulerService {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final RabbitTemplate rabbitTemplate;

    @Value("${scheduler.batch-size:50}")
    private int batchSize;

    /**
     * Main event loop — polls for due jobs and dispatches them to the message broker.
     *
     * <p>Uses {@code fixedDelay} (not {@code fixedRate}) so a slow DB cycle never causes
     * overlapping invocations within the same Scheduler instance.
     */
    @Scheduled(fixedDelayString = "${scheduler.poll-interval-ms:5000}")
    @Transactional
    public void pollAndDispatch() {
        Instant now = Instant.now();

        // ── 1. Acquire locks on all due jobs ────────────────────────────────
        List<JobDefinition> dueJobs = jobDefinitionRepository.findDueJobsWithLock(now, batchSize);

        if (dueJobs.isEmpty()) {
            log.trace("No due jobs found at {}", now);
            return;
        }

        log.info("Scheduler acquired locks on {} due job(s) at {}", dueJobs.size(), now);

        for (JobDefinition job : dueJobs) {
            try {
                dispatchJob(job, now);
            } catch (Exception ex) {
                // Log per-job failures without aborting the entire batch.
                // The transaction will still commit for all successfully dispatched jobs.
                log.error("Failed to dispatch job '{}' (id={}): {}",
                        job.getName(), job.getId(), ex.getMessage(), ex);
            }
        }
    }

    /**
     * Advances the job's {@code next_execution_time} using the drift-resistant formula and
     * schedules a post-commit RabbitMQ publish.
     *
     * @param job the due job (already locked in the current transaction)
     * @param now the timestamp captured at the start of this polling cycle
     */
    private void dispatchJob(JobDefinition job, Instant now) {

        // ── 2. Compute next slot (drift-resistant) ───────────────────────────
        Instant nextRun = nextAnchoredSlot(job, now);
        job.setNextExecutionTime(nextRun);
        jobDefinitionRepository.save(job);

        // ── 3. Build the message to be delivered to the Worker ──────────────
        JobTaskMessage message = JobTaskMessage.builder()
                .jobId(job.getId())
                .jobName(job.getName())
                .targetUrl(job.getTargetUrl())
                .httpMethod(job.getHttpMethod())
                .scheduledTime(now)          // The "should have fired at" timestamp
                .build();

        log.debug("Dispatched job '{}' (id={}): next run at {} UTC",
                job.getName(), job.getId(), nextRun);

        // ── 4. Publish to RabbitMQ AFTER the transaction commits ─────────────
        //
        // Using afterCommit() guarantees that:
        //   a) If the DB transaction rolls back → no message is ever sent.
        //   b) The row lock is released before the broker call → no lock contention.
        //
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    rabbitTemplate.convertAndSend(
                            RabbitMQConfig.EXCHANGE_NAME,
                            RabbitMQConfig.ROUTING_KEY,
                            message);
                    log.info("Published job task for '{}' (id={}) to broker",
                            job.getName(), job.getId());
                } catch (Exception ex) {
                    // The DB is already committed; log the miss so it can be investigated.
                    // The job will execute again at its next anchored slot.
                    log.error("BROKER PUBLISH FAILED for job '{}' (id={}): {}. "
                            + "Job will be rescheduled at its next anchored slot.",
                            job.getName(), job.getId(), ex.getMessage(), ex);
                }
            }
        });
    }

    /**
     * Computes the next execution slot strictly after {@code now} using the
     * drift-resistant, anchor-based formula.
     *
     * <pre>
     *   anchor    = today (IST) at job.startHour:job.startMinute:00
     *   intervalMs = job.intervalMinutes × 60 000
     *   elapsedMs  = now.epochMilli − anchor.epochMilli
     *   slots      = ceil(elapsedMs / intervalMs)   // always ≥ 1
     *   nextRun    = anchor + slots × intervalMs
     * </pre>
     *
     * <p>If {@code elapsedMs ≤ 0} (anchor is still in the future today), {@code slots = 1}
     * so {@code nextRun = anchor + 1 × interval}, which is still a future slot.
     *
     * @param job the job definition containing anchor and interval
     * @param now current time
     * @return the next clean slot after {@code now}
     */
    static Instant nextAnchoredSlot(JobDefinition job, Instant now) {
        // Build today's anchor in IST using the passed 'now' timestamp
        ZonedDateTime anchor = ZonedDateTime.ofInstant(now, java.time.ZoneId.of("Asia/Kolkata"))
                .withHour(job.getStartHour())
                .withMinute(job.getStartMinute())
                .withSecond(0)
                .withNano(0);

        long intervalMs   = (long) job.getIntervalMinutes() * 60_000L;
        long anchorEpoch  = anchor.toInstant().toEpochMilli();
        long nowEpoch     = now.toEpochMilli();
        long elapsedMs    = nowEpoch - anchorEpoch;

        // Number of complete (or partial) intervals elapsed since anchor — at least 1
        long slots = (elapsedMs <= 0)
                ? 1L
                : (long) Math.ceil((double) elapsedMs / intervalMs);

        return Instant.ofEpochMilli(anchorEpoch + slots * intervalMs);
    }
}
