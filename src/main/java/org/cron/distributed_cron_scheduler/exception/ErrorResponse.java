package org.cron.distributed_cron_scheduler.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Standard error envelope returned by the {@link GlobalExceptionHandler}
 * for all 4xx and 5xx responses.
 *
 * <p>Example response body:
 * <pre>
 * {
 *   "code":      "NOT_FOUND",
 *   "message":   "Job not found with id: 3fa85f64-5717-4562-b3fc-2c963f66afa6",
 *   "timestamp": "2026-06-20T05:00:00Z"
 * }
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {

    /** Machine-readable error code (e.g. NOT_FOUND, VALIDATION_ERROR, INTERNAL_ERROR). */
    private String  code;

    /** Human-readable explanation suitable for display in a client application. */
    private String  message;

    /** UTC timestamp when the error occurred. */
    private Instant timestamp;

    /**
     * Convenience constructor that automatically sets {@link #timestamp} to now.
     */
    public ErrorResponse(String code, String message) {
        this.code      = code;
        this.message   = message;
        this.timestamp = Instant.now();
    }
}
