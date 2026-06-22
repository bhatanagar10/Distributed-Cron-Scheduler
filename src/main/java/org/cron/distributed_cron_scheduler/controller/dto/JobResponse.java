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

    /** IST hour (0–23) of the daily anchor time. */
    private Integer startHour;

    /** IST minute (0–59) of the daily anchor time. */
    private Integer startMinute;

    /** Recurrence interval in minutes (1–1440). */
    private Integer intervalMinutes;

    private Boolean enabled;
    private Instant nextExecutionTime;
    private Instant createdAt;
    private Instant updatedAt;
}
