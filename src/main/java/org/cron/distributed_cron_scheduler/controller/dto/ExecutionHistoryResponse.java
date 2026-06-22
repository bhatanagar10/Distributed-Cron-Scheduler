package org.cron.distributed_cron_scheduler.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Outbound DTO for a single row from the {@code job_execution_history} table.
 *
 * <p>Returned by:
 * <ul>
 *   <li>{@code GET /api/jobs/{id}/history} — full execution log</li>
 *   <li>{@code GET /api/jobs/{id}/delays}  — delay-filtered log</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExecutionHistoryResponse {

    /** UUID of this execution record. */
    private UUID    id;

    /** UUID of the parent job. */
    private UUID    jobId;

    /** When the job was supposed to fire (set by the Scheduler). */
    private Instant scheduledTime;

    /** When the Worker actually began executing the HTTP call. */
    private Instant actualStartTime;

    /**
     * Scheduling drift in milliseconds (computed by PostgreSQL).
     * A value of {@code 1500} means the job started 1.5 seconds late.
     */
    private Long    delayMs;

    /** Execution outcome: SUCCESS, FAILED, or TIMEOUT. */
    private String  status;

    /** HTTP status code returned by the target endpoint (null on network failure). */
    private Integer httpStatusCode;

    /** First 10 KB of the HTTP response body, or an error description. */
    private String  responsePayload;

    private Instant createdAt;
}
