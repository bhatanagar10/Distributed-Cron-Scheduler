package org.cron.distributed_cron_scheduler.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.controller.dto.ExecutionHistoryResponse;
import org.cron.distributed_cron_scheduler.controller.dto.JobRequest;
import org.cron.distributed_cron_scheduler.controller.dto.JobResponse;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.cron.distributed_cron_scheduler.domain.JobExecutionHistory;
import org.cron.distributed_cron_scheduler.exception.JobNotFoundException;
import org.cron.distributed_cron_scheduler.repository.JobDefinitionRepository;
import org.cron.distributed_cron_scheduler.repository.JobExecutionHistoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Business logic layer for the Distributed Cron Scheduler REST API.
 *
 * <h3>Scheduling model</h3>
 * <p>Jobs fire at slots derived from a fixed daily <em>anchor</em>:
 * <pre>
 *   anchor   = today (IST) at startHour:startMinute:00
 *   slot(n)  = anchor + n × intervalMinutes
 * </pre>
 * The first run for a newly created or updated job is the <em>smallest future slot</em>
 * strictly after the current time — computed by {@link #firstFutureSlot(int, int, int)}.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Create, read, update, and delete {@code JobDefinition} records.</li>
 *   <li>Map between entities and DTOs (no Jackson annotations leak into the domain layer).</li>
 *   <li>Query execution history and delay-monitoring data.</li>
 * </ul>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class JobService {

    private final JobDefinitionRepository       jobDefinitionRepository;
    private final JobExecutionHistoryRepository historyRepository;

    @Value("${scheduler.delay-threshold-ms:2000}")
    private long defaultDelayThresholdMs;

    // ── CRUD Operations ──────────────────────────────────────────────────────

    /**
     * Creates a new job and schedules its first run at the next future anchor-time slot.
     *
     * @param request validated inbound DTO
     * @return the persisted job as a {@link JobResponse}
     */
    @Transactional
    public JobResponse createJob(JobRequest request) {
        Instant firstRun = firstFutureSlot(
                request.getStartHour(),
                request.getStartMinute(),
                request.getIntervalMinutes());

        JobDefinition job = JobDefinition.builder()
                .name(request.getName())
                .targetUrl(request.getTargetUrl())
                .httpMethod(request.getHttpMethod())
                .startHour(request.getStartHour())
                .startMinute(request.getStartMinute())
                .intervalMinutes(request.getIntervalMinutes())
                .enabled(request.getEnabled() != null ? request.getEnabled() : Boolean.TRUE)
                .nextExecutionTime(firstRun)
                .build();

        JobDefinition saved = jobDefinitionRepository.save(job);
        log.info("Created job '{}' (id={}) — first run at {} UTC",
                saved.getName(), saved.getId(), saved.getNextExecutionTime());
        return toResponse(saved);
    }

    /**
     * Returns all registered job definitions.
     */
    @Transactional(readOnly = true)
    public List<JobResponse> getAllJobs() {
        return jobDefinitionRepository.findAll()
                .stream()
                .map(this::toResponse)
                .toList();
    }

    /**
     * Returns a single job by ID.
     *
     * @throws JobNotFoundException if no job exists with the given ID
     */
    @Transactional(readOnly = true)
    public JobResponse getJobById(UUID id) {
        return toResponse(findOrThrow(id));
    }

    /**
     * Updates an existing job's mutable properties.
     *
     * <p>If the schedule configuration (anchor time or interval) changes,
     * {@code next_execution_time} is recalculated from the new anchor so the
     * updated schedule takes effect immediately.
     *
     * @throws JobNotFoundException if no job exists with the given ID
     */
    @Transactional
    public JobResponse updateJob(UUID id, JobRequest request) {
        JobDefinition job = findOrThrow(id);

        boolean scheduleChanged =
                !job.getStartHour().equals(request.getStartHour())       ||
                !job.getStartMinute().equals(request.getStartMinute())   ||
                !job.getIntervalMinutes().equals(request.getIntervalMinutes());

        job.setName(request.getName());
        job.setTargetUrl(request.getTargetUrl());
        job.setHttpMethod(request.getHttpMethod());
        job.setStartHour(request.getStartHour());
        job.setStartMinute(request.getStartMinute());
        job.setIntervalMinutes(request.getIntervalMinutes());
        job.setEnabled(request.getEnabled() != null ? request.getEnabled() : Boolean.TRUE);

        if (scheduleChanged) {
            Instant newFirstRun = firstFutureSlot(
                    request.getStartHour(),
                    request.getStartMinute(),
                    request.getIntervalMinutes());
            job.setNextExecutionTime(newFirstRun);
            log.info("Job '{}' schedule changed — next run rescheduled to {} UTC",
                    job.getName(), newFirstRun);
        }

        return toResponse(jobDefinitionRepository.save(job));
    }

    /**
     * Permanently deletes a job and all its execution history (via CASCADE).
     *
     * @throws JobNotFoundException if no job exists with the given ID
     */
    @Transactional
    public void deleteJob(UUID id) {
        JobDefinition job = findOrThrow(id);
        jobDefinitionRepository.delete(job);
        log.info("Deleted job '{}' (id={})", job.getName(), id);
    }

    // ── Observability Queries ────────────────────────────────────────────────

    /**
     * Returns the complete execution history for a job, newest-first.
     *
     * @throws JobNotFoundException if the parent job does not exist
     */
    @Transactional(readOnly = true)
    public List<ExecutionHistoryResponse> getJobHistory(UUID jobId) {
        findOrThrow(jobId); // validate parent exists
        return historyRepository.findByJobIdOrderByCreatedAtDesc(jobId)
                .stream()
                .map(this::toHistoryResponse)
                .toList();
    }

    /**
     * Returns only executions where the scheduling drift ({@code delay_ms}) exceeded
     * {@code thresholdMs}. Falls back to {@link #defaultDelayThresholdMs} if not supplied.
     *
     * @param jobId       the parent job UUID
     * @param thresholdMs override delay threshold in milliseconds (null → use app default)
     * @throws JobNotFoundException if the parent job does not exist
     */
    @Transactional(readOnly = true)
    public List<ExecutionHistoryResponse> getDelayedExecutions(UUID jobId, Long thresholdMs) {
        findOrThrow(jobId); // validate parent exists
        long threshold = (thresholdMs != null) ? thresholdMs : defaultDelayThresholdMs;
        return historyRepository.findDelayedExecutions(jobId, threshold)
                .stream()
                .map(this::toHistoryResponse)
                .toList();
    }

    // ── Scheduling helpers ───────────────────────────────────────────────────

    /**
     * Computes the first execution slot that is strictly in the future.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Build the anchor for <em>today</em> in UTC: {@code today@startHour:startMinute:00}.</li>
     *   <li>Walk forward in {@code intervalMinutes} steps until the slot is after {@code now}.</li>
     *   <li>If no slot on today is in the future (anchor + N steps wraps past midnight),
     *       the anchor for <em>tomorrow</em> is the first slot.</li>
     * </ol>
     *
     * <p>Example: startHour=10, startMinute=0, intervalMinutes=5, now=10:07
     * → anchor=10:00 → 10:05 (past) → 10:10 ✓  (returned)
     *
     * @param startHour       0–23 IST hour of the daily anchor
     * @param startMinute     0–59 IST minute of the daily anchor
     * @param intervalMinutes 1–1440 recurrence period
     * @return the earliest future slot as a UTC {@link Instant}
     */
    public static Instant firstFutureSlot(int startHour, int startMinute, int intervalMinutes) {
        Instant now = Instant.now();
        ZonedDateTime anchor = ZonedDateTime.ofInstant(now, java.time.ZoneId.of("Asia/Kolkata"))
                .withHour(startHour)
                .withMinute(startMinute)
                .withSecond(0)
                .withNano(0);

        long intervalMs = (long) intervalMinutes * 60_000L;

        // Advance anchor until it is strictly after now
        while (!anchor.toInstant().isAfter(now)) {
            anchor = anchor.plusMinutes(intervalMinutes);

            // Guard: if we've looped past midnight into the next day, anchor is already future
            if (anchor.toInstant().toEpochMilli() - now.toEpochMilli() > 24L * 60 * 60 * 1000) {
                break;
            }
        }

        return anchor.toInstant();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private JobDefinition findOrThrow(UUID id) {
        return jobDefinitionRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
    }

    private JobResponse toResponse(JobDefinition job) {
        return JobResponse.builder()
                .id(job.getId())
                .name(job.getName())
                .targetUrl(job.getTargetUrl())
                .httpMethod(job.getHttpMethod())
                .startHour(job.getStartHour())
                .startMinute(job.getStartMinute())
                .intervalMinutes(job.getIntervalMinutes())
                .enabled(job.getEnabled())
                .nextExecutionTime(job.getNextExecutionTime())
                .createdAt(job.getCreatedAt())
                .updatedAt(job.getUpdatedAt())
                .build();
    }

    private ExecutionHistoryResponse toHistoryResponse(JobExecutionHistory h) {
        return ExecutionHistoryResponse.builder()
                .id(h.getId())
                .jobId(h.getJob().getId())
                .scheduledTime(h.getScheduledTime())
                .actualStartTime(h.getActualStartTime())
                .delayMs(h.getDelayMs())
                .status(h.getStatus().name())
                .httpStatusCode(h.getHttpStatusCode())
                .responsePayload(h.getResponsePayload())
                .createdAt(h.getCreatedAt())
                .build();
    }
}
