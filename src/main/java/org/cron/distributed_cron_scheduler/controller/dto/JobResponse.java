package org.cron.distributed_cron_scheduler.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Outbound DTO representing a {@code JobDefinition} in REST API responses.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobResponse {

    private UUID    id;
    private String  name;
    private String  targetUrl;
    private String  httpMethod;

    /**
     * 5-field Linux cron expression interpreted in IST (Asia/Kolkata).
     * Example: every 5 minutes = "&#42;/5 * * * *"
     */
    private String  cronExpression;

    /**
     * Human-readable English description of the cron expression.
     * Example: "every 5 minutes" or "at 10:00 AM, Monday through Friday".
     */
    private String  cronDescription;

    private Boolean enabled;
    private Instant nextExecutionTime;
    private Instant createdAt;
    private Instant updatedAt;
}
