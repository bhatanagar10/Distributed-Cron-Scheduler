package org.cron.distributed_cron_scheduler.repository;

import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Repository for {@link JobDefinition} entities.
 *
 * <p>The most critical method here is {@link #findDueJobsWithLock}, which uses a
 * PostgreSQL-specific native query combining:
 * <ul>
 *   <li>{@code FOR UPDATE} – acquires a row-level exclusive lock on each returned row</li>
 *   <li>{@code SKIP LOCKED} – skips rows already locked by another transaction</li>
 * </ul>
 *
 * <p>This combination allows multiple Scheduler (Manager) instances to safely poll
 * the same table concurrently with <b>zero duplicate dispatches</b>:
 * each instance grabs a disjoint set of due jobs and holds their locks until the
 * transaction commits (which also updates {@code next_execution_time}).
 */
@Repository
public interface JobDefinitionRepository extends JpaRepository<JobDefinition, UUID> {

    /**
     * Atomically selects up to {@code limit} enabled jobs whose scheduled time
     * has arrived, acquiring exclusive row locks and skipping any rows locked
     * by a concurrent Scheduler instance.
     *
     * <p>This query <strong>must</strong> be called inside a {@code @Transactional}
     * method, otherwise the {@code FOR UPDATE} clause is a no-op.
     *
     * @param now   current UTC timestamp (jobs with {@code next_execution_time <= now} qualify)
     * @param limit maximum number of rows to return per poll cycle (prevents long lock holds)
     * @return list of due jobs, each exclusively locked for the duration of the caller's transaction
     */
    @Query(
        value = """
            SELECT *
              FROM job_definitions
             WHERE enabled = true
               AND next_execution_time <= :now
             ORDER BY next_execution_time ASC
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """,
        nativeQuery = true
    )
    List<JobDefinition> findDueJobsWithLock(@Param("now") Instant now,
                                             @Param("limit") int limit);
}
