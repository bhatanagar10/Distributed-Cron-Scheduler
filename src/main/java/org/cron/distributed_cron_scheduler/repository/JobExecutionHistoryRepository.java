package org.cron.distributed_cron_scheduler.repository;

import org.cron.distributed_cron_scheduler.domain.JobExecutionHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Repository for querying the immutable {@link JobExecutionHistory} audit log.
 *
 * <p>Two primary read patterns are exposed:
 * <ol>
 *   <li><b>Full history</b> – all execution records for a given job, sorted newest-first.</li>
 *   <li><b>Delay monitoring</b> – records where the scheduling drift exceeded a threshold,
 *       enabling operators to identify slow-starting or backlogged jobs.</li>
 * </ol>
 */
@Repository
public interface JobExecutionHistoryRepository extends JpaRepository<JobExecutionHistory, UUID> {

    /**
     * Returns the complete execution history for a specific job, ordered newest-first.
     *
     * <p>Used by {@code GET /api/jobs/{id}/history}.
     *
     * @param jobId the UUID of the parent {@link org.cron.distributed_cron_scheduler.domain.JobDefinition}
     * @return all execution records, sorted by {@code created_at DESC}
     */
    @Query("""
            SELECT h FROM JobExecutionHistory h
             WHERE h.job.id = :jobId
             ORDER BY h.createdAt DESC
           """)
    List<JobExecutionHistory> findByJobIdOrderByCreatedAtDesc(@Param("jobId") UUID jobId);

    /**
     * Returns only the execution records where the scheduling drift ({@code delay_ms})
     * exceeded the given threshold. This is the core of the delay-monitoring feature.
     *
     * <p>Used by {@code GET /api/jobs/{id}/delays?thresholdMs=2000}.
     *
     * @param jobId       the UUID of the parent job definition
     * @param thresholdMs minimum drift in milliseconds to be included in the result
     * @return delayed execution records, sorted by drift descending (worst offenders first)
     */
    @Query("""
            SELECT h FROM JobExecutionHistory h
             WHERE h.job.id   = :jobId
               AND h.delayMs  > :thresholdMs
             ORDER BY h.delayMs DESC
           """)
    List<JobExecutionHistory> findDelayedExecutions(@Param("jobId") UUID jobId,
                                                     @Param("thresholdMs") long thresholdMs);
}
