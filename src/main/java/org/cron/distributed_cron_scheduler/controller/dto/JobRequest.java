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
 * <p>Jobs are defined by an <em>anchor time</em> ({@code startHour}:{@code startMinute} IST)
 * and a repeating {@code intervalMinutes}. All execution slots are derived deterministically:
 * <pre>
 *   anchor → anchor + interval → anchor + 2×interval → …
 * </pre>
 * Example: {@code startHour=10, startMinute=0, intervalMinutes=5}
 * → runs at 10:00, 10:05, 10:10, … 23:55 (IST) every day.
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
     * IST (India Standard Time) hour of the daily anchor time (0 = midnight, 23 = 11 PM).
     * Combined with {@link #startMinute} this defines when the first slot fires each day.
     */
    @NotNull(message = "Start hour is required")
    @Min(value = 0,  message = "Start hour must be between 0 and 23")
    @Max(value = 23, message = "Start hour must be between 0 and 23")
    private Integer startHour;

    /**
     * IST (India Standard Time) minute of the daily anchor time (0–59).
     */
    @NotNull(message = "Start minute is required")
    @Min(value = 0,  message = "Start minute must be between 0 and 59")
    @Max(value = 59, message = "Start minute must be between 0 and 59")
    private Integer startMinute;

    /**
     * Recurrence interval in minutes.
     * Minimum: 1 minute · Maximum: 1440 minutes (24 hours).
     */
    @NotNull(message = "Interval in minutes is required")
    @Min(value = 1,    message = "Interval must be at least 1 minute")
    @Max(value = 1440, message = "Interval must not exceed 1440 minutes (24 hours)")
    private Integer intervalMinutes;

    /**
     * Whether the job should be actively scheduled. Defaults to {@code true}.
     */
    @Builder.Default
    private Boolean enabled = Boolean.TRUE;
}
