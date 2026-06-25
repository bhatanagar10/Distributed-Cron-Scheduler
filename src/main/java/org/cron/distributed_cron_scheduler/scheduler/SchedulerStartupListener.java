package org.cron.distributed_cron_scheduler.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.cron.distributed_cron_scheduler.repository.JobDefinitionRepository;
import org.cron.distributed_cron_scheduler.service.RedisScheduleService;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Rebuilds the Redis ZSET scheduling index from PostgreSQL on every application startup.
 *
 * <h3>Why this is needed</h3>
 * <p>Redis is an in-memory store. If it restarts (crash, upgrade, eviction), all
 * scheduled job entries in the ZSET are lost. Without a rebuild, no jobs would
 * ever fire again until they are manually re-registered.
 *
 * <h3>How it works</h3>
 * <p>On {@link ApplicationReadyEvent} (fired after the application context is fully
 * initialized and the server is ready to serve traffic), this listener:
 * <ol>
 *   <li>Queries PostgreSQL for all enabled {@link JobDefinition}s.</li>
 *   <li>Uses the cached {@code next_execution_time} column as the ZSET score.</li>
 *   <li>Calls {@link RedisScheduleService#rebuildFromDatabase} which does a bulk
 *       {@code ZADD} — idempotent if entries already exist.</li>
 * </ol>
 *
 * <p>If {@code next_execution_time} is in the past for a job (e.g. the app was down
 * for several hours), it will immediately appear as "due" on the first poll cycle,
 * and the Scheduler will compute the correct next slot from the cron expression.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SchedulerStartupListener {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final RedisScheduleService    redisScheduleService;

    /**
     * Triggered once after the Spring application context is fully started.
     * Rebuilds the Redis ZSET from the Postgres {@code job_definitions} table.
     */
    @EventListener(ApplicationReadyEvent.class)
    @Transactional(readOnly = true)
    public void rebuildRedisSchedule() {
        log.info("ApplicationReadyEvent received — rebuilding Redis ZSET from PostgreSQL...");

        List<JobDefinition> enabledJobs = jobDefinitionRepository.findAllByEnabledTrue();
        redisScheduleService.rebuildFromDatabase(enabledJobs);

        log.info("Redis ZSET rebuild complete — {} enabled job(s) registered.", enabledJobs.size());
    }
}
