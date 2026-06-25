package org.cron.distributed_cron_scheduler.controller.dto;

import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Inbound DTO for creating or updating a {@code JobDefinition}.
 *
 * <h3>Scheduling model</h3>
 * <p>Jobs are scheduled using a standard 5-field Linux cron expression interpreted
 * in <strong>IST (Asia/Kolkata, UTC+5:30)</strong>.
 *
 * <h3>Cron expression format</h3>
 * <pre>
 *   ┌───── minute       (0–59)
 *   │ ┌─── hour         (0–23)
 *   │ │ ┌─ day-of-month (1–31)
 *   │ │ │ ┌ month       (1–12)
 *   │ │ │ │ ┌ day-of-week (0–7, 0 and 7 = Sunday)
 *   * * * * *
 * </pre>
 *
 * <h3>Examples</h3>
 * <ul>
 *   <li>"&#42;/5 * * * *" &mdash; every 5 minutes</li>
 *   <li>"0 10 * * 1-5" &mdash; 10:00 AM IST, Monday-Friday</li>
 *   <li>"30 9 1 * *" &mdash; 09:30 IST on the 1st of every month</li>
 *   <li>"0 &#42;/2 * * *" &mdash; every 2 hours</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobRequest {

    /**
     * Unique, human-readable identifier for this job.
     * Example: {@code "payment-gateway-health-check"}
     */
    @NotBlank(message = "Job name must not be blank")
    @Size(max = 255, message = "Job name must not exceed 255 characters")
    private String name;

    /**
     * Fully-qualified HTTP/HTTPS URL of the target endpoint.
     * Example: {@code "https://api.example.com/health"}
     */
    @NotBlank(message = "Target URL must not be blank")
    @Pattern(
        regexp = "^https?://.*",
        message = "Target URL must start with http:// or https://"
    )
    private String targetUrl;

    /**
     * HTTP verb to use when calling the target URL.
     * Must be one of: {@code GET}, {@code POST}, {@code PUT}, {@code DELETE}, {@code PATCH}.
     */
    @NotBlank(message = "HTTP method must not be blank")
    @Pattern(
        regexp = "^(GET|POST|PUT|DELETE|PATCH)$",
        message = "HTTP method must be one of: GET, POST, PUT, DELETE, PATCH"
    )
    private String httpMethod;

    /**
     * Standard 5-field Linux cron expression interpreted in IST.
     * Must be a valid UNIX cron expression (no seconds field).
     * Example: every 5 minutes = "&#42;/5 * * * *"
     */
    @NotBlank(message = "Cron expression must not be blank")
    private String cronExpression;

    /**
     * Whether the job should be actively scheduled. Defaults to {@code true}.
     */
    @Builder.Default
    private Boolean enabled = Boolean.TRUE;
}
