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
import java.util.List;
import java.util.UUID;

/**
 * Business logic layer for the Distributed Cron Scheduler REST API.
 *
 * <h3>Scheduling model</h3>
 * <p>Jobs are scheduled using a standard 5-field Linux cron expression interpreted
 * in <strong>IST (Asia/Kolkata)</strong>. The next execution time is computed via
 * {@link CronService} and stored in both PostgreSQL ({@code next_execution_time})
 * and the Redis ZSET scheduling index via {@link RedisScheduleService}.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Create, read, update, and delete {@code JobDefinition} records.</li>
 *   <li>Keep the Redis ZSET in sync on every CRUD operation.</li>
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
    private final CronService                   cronService;
    private final RedisScheduleService          redisScheduleService;

    @Value("${scheduler.delay-threshold-ms:2000}")
    private long defaultDelayThresholdMs;

    // ── CRUD Operations ──────────────────────────────────────────────────────

    /**
     * Creates a new job, schedules its first run using the cron expression,
     * and registers it in the Redis ZSET.
     *
     * @param request validated inbound DTO
     * @return the persisted job as a {@link JobResponse}
     * @throws IllegalArgumentException if the cron expression is invalid
     */
    @Transactional
    public JobResponse createJob(JobRequest request) {
        if (!cronService.isValid(request.getCronExpression())) {
            throw new IllegalArgumentException(
                    "Invalid cron expression: '" + request.getCronExpression() + "'");
        }

        Instant firstRun = cronService.nextExecution(request.getCronExpression(), Instant.now());

        JobDefinition job = JobDefinition.builder()
                .name(request.getName())
                .targetUrl(request.getTargetUrl())
                .httpMethod(request.getHttpMethod())
                .cronExpression(request.getCronExpression())
                .enabled(request.getEnabled() != null ? request.getEnabled() : Boolean.TRUE)
                .nextExecutionTime(firstRun)
                .build();

        JobDefinition saved = jobDefinitionRepository.save(job);

        // Register in Redis ZSET (only if enabled)
        if (Boolean.TRUE.equals(saved.getEnabled())) {
            redisScheduleService.scheduleJob(saved.getId().toString(), firstRun.toEpochMilli());
        }

        log.info("Created job '{}' (id={}) — cron='{}', first run at {} IST",
                saved.getName(), saved.getId(),
                saved.getCronExpression(), firstRun);
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
     * <p>If the cron expression or enabled flag changes, the Redis ZSET is updated
     * immediately so the new schedule takes effect on the very next poll cycle.
     *
     * @throws JobNotFoundException     if no job exists with the given ID
     * @throws IllegalArgumentException if the new cron expression is invalid
     */
    @Transactional
    public JobResponse updateJob(UUID id, JobRequest request) {
        if (!cronService.isValid(request.getCronExpression())) {
            throw new IllegalArgumentException(
                    "Invalid cron expression: '" + request.getCronExpression() + "'");
        }

        JobDefinition job = findOrThrow(id);

        boolean cronChanged    = !job.getCronExpression().equals(request.getCronExpression());
        boolean enabledChanged = !job.getEnabled().equals(
                request.getEnabled() != null ? request.getEnabled() : Boolean.TRUE);

        job.setName(request.getName());
        job.setTargetUrl(request.getTargetUrl());
        job.setHttpMethod(request.getHttpMethod());
        job.setCronExpression(request.getCronExpression());
        job.setEnabled(request.getEnabled() != null ? request.getEnabled() : Boolean.TRUE);

        if (cronChanged) {
            Instant newFirstRun = cronService.nextExecution(request.getCronExpression(), Instant.now());
            job.setNextExecutionTime(newFirstRun);
            log.info("Job '{}' cron changed to '{}' — next run at {} IST",
                    job.getName(), request.getCronExpression(), newFirstRun);
        }

        JobDefinition saved = jobDefinitionRepository.save(job);

        // Sync Redis ZSET
        if (Boolean.TRUE.equals(saved.getEnabled())) {
            redisScheduleService.scheduleJob(
                    saved.getId().toString(),
                    saved.getNextExecutionTime().toEpochMilli());
        } else {
            // Job was disabled — remove from scheduling index
            redisScheduleService.removeJob(saved.getId().toString());
            log.info("Job '{}' disabled — removed from Redis ZSET", saved.getName());
        }

        return toResponse(saved);
    }

    /**
     * Permanently deletes a job, removes it from the Redis ZSET,
     * and cascades the deletion to all execution history.
     *
     * @throws JobNotFoundException if no job exists with the given ID
     */
    @Transactional
    public void deleteJob(UUID id) {
        JobDefinition job = findOrThrow(id);
        jobDefinitionRepository.delete(job);
        redisScheduleService.removeJob(id.toString());
        log.info("Deleted job '{}' (id={}) and removed from Redis ZSET", job.getName(), id);
    }

    // ── Observability Queries ────────────────────────────────────────────────

    /**
     * Returns the complete execution history for a job, newest-first.
     *
     * @throws JobNotFoundException if the parent job does not exist
     */
    @Transactional(readOnly = true)
    public List<ExecutionHistoryResponse> getJobHistory(UUID jobId) {
        findOrThrow(jobId);
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
        findOrThrow(jobId);
        long threshold = (thresholdMs != null) ? thresholdMs : defaultDelayThresholdMs;
        return historyRepository.findDelayedExecutions(jobId, threshold)
                .stream()
                .map(this::toHistoryResponse)
                .toList();
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
                .cronExpression(job.getCronExpression())
                .cronDescription(cronService.describe(job.getCronExpression()))
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
