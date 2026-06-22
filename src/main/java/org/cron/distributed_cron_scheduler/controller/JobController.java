package org.cron.distributed_cron_scheduler.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.cron.distributed_cron_scheduler.controller.dto.ExecutionHistoryResponse;
import org.cron.distributed_cron_scheduler.controller.dto.JobRequest;
import org.cron.distributed_cron_scheduler.controller.dto.JobResponse;
import org.cron.distributed_cron_scheduler.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST controller exposing all public endpoints for the Distributed Cron Scheduler.
 *
 * <h3>Endpoint summary</h3>
 * <pre>
 *   POST   /api/jobs                       — Create a new job
 *   GET    /api/jobs                       — List all jobs
 *   GET    /api/jobs/{id}                  — Get a single job
 *   PUT    /api/jobs/{id}                  — Update a job
 *   DELETE /api/jobs/{id}                  — Delete a job (cascades to history)
 *
 *   GET    /api/jobs/{id}/history          — Full execution history (newest-first)
 *   GET    /api/jobs/{id}/delays           — Executions where drift > threshold
 * </pre>
 */
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    // ── Job CRUD ─────────────────────────────────────────────────────────────

    /**
     * Create a new cron job.
     *
     * <p>The job's first execution is the earliest future slot derived from the
     * provided {@code startHour}:{@code startMinute} anchor and {@code intervalMinutes}.
     *
     * @param request validated job definition
     * @return 201 Created with the persisted job in the body
     */
    @PostMapping
    public ResponseEntity<JobResponse> createJob(@Valid @RequestBody JobRequest request) {
        JobResponse response = jobService.createJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * List all registered job definitions.
     *
     * @return 200 OK with a (possibly empty) list of jobs
     */
    @GetMapping
    public ResponseEntity<List<JobResponse>> getAllJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }

    /**
     * Get a single job by its UUID.
     *
     * @param id the job's UUID
     * @return 200 OK with the job, or 404 if not found
     */
    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJobById(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.getJobById(id));
    }

    /**
     * Update an existing job.
     *
     * <p>If the schedule configuration ({@code startHour}, {@code startMinute},
     * or {@code intervalMinutes}) changes, {@code next_execution_time} is immediately
     * recalculated from the new anchor so the updated cadence takes effect on the
     * next poll cycle.
     *
     * @param id      the UUID of the job to update
     * @param request the updated job definition
     * @return 200 OK with the updated job, or 404 if not found
     */
    @PutMapping("/{id}")
    public ResponseEntity<JobResponse> updateJob(@PathVariable UUID id,
                                                  @Valid @RequestBody JobRequest request) {
        return ResponseEntity.ok(jobService.updateJob(id, request));
    }

    /**
     * Delete a job permanently.
     * All associated execution history is removed via the database CASCADE.
     *
     * @param id the UUID of the job to delete
     * @return 204 No Content on success, or 404 if not found
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable UUID id) {
        jobService.deleteJob(id);
        return ResponseEntity.noContent().build();
    }

    // ── Observability ────────────────────────────────────────────────────────

    /**
     * Retrieve the complete execution history for a specific job.
     *
     * <p>Results are sorted newest-first, so the most recent run appears at index 0.
     *
     * @param id the UUID of the parent job
     * @return 200 OK with an ordered list of execution records
     */
    @GetMapping("/{id}/history")
    public ResponseEntity<List<ExecutionHistoryResponse>> getJobHistory(@PathVariable UUID id) {
        return ResponseEntity.ok(jobService.getJobHistory(id));
    }

    /**
     * Retrieve executions where the scheduling drift exceeded a threshold.
     *
     * <p>Results are sorted by {@code delay_ms} descending (worst offenders first),
     * making it easy to identify chronically delayed jobs at a glance.
     *
     * @param id          the UUID of the parent job
     * @param thresholdMs drift threshold in milliseconds (optional; defaults to
     *                    {@code scheduler.delay-threshold-ms} from {@code application.yaml})
     * @return 200 OK with delayed executions, or an empty list if none exceed the threshold
     */
    @GetMapping("/{id}/delays")
    public ResponseEntity<List<ExecutionHistoryResponse>> getDelayedExecutions(
            @PathVariable UUID id,
            @RequestParam(required = false) Long thresholdMs) {
        return ResponseEntity.ok(jobService.getDelayedExecutions(id, thresholdMs));
    }
}
