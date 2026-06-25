package org.cron.distributed_cron_scheduler.repository;

import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Repository for {@link JobDefinition} entities.
 *
 * <p>PostgreSQL is the source of truth for job definitions.
 * The actual scheduling index lives in a Redis ZSET ({@code cron:schedule}).
 *
 * <p>{@link #findAllByEnabledTrue()} is used by the startup listener to
 * rebuild the Redis ZSET after a Redis restart, ensuring no jobs are lost.
 */
@Repository
public interface JobDefinitionRepository extends JpaRepository<JobDefinition, UUID> {

    /**
     * Returns all enabled job definitions.
     * Used by {@code SchedulerStartupListener} to rebuild the Redis ZSET on application startup.
     *
     * @return list of all jobs where {@code enabled = true}
     */
    List<JobDefinition> findAllByEnabledTrue();
}
