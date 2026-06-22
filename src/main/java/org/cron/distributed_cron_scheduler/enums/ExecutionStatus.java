package org.cron.distributed_cron_scheduler.enums;

/**
 * Represents the final outcome of a single job execution attempt.
 *
 * <ul>
 *   <li>{@link #SUCCESS} – The target returned a 2xx HTTP response within the timeout window.</li>
 *   <li>{@link #FAILED}  – The target returned a non-2xx response, or a network-level error occurred.</li>
 *   <li>{@link #TIMEOUT} – The target did not respond within the configured {@code scheduler.http-timeout-seconds}.</li>
 * </ul>
 */
public enum ExecutionStatus {
    SUCCESS,
    FAILED,
    TIMEOUT
}
