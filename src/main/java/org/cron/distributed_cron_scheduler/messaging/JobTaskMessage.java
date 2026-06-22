package org.cron.distributed_cron_scheduler.messaging;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Immutable message published to the {@code cron.exchange} RabbitMQ exchange
 * by the Scheduler (Manager) and consumed by the Execution Engine (Workers).
 *
 * <p>Contains everything the Worker needs to:
 * <ol>
 *   <li>Execute the HTTP call ({@link #targetUrl}, {@link #httpMethod})</li>
 *   <li>Record the execution result ({@link #jobId}, {@link #scheduledTime})</li>
 * </ol>
 *
 * <p>Serialized as JSON via {@code Jackson2JsonMessageConverter}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobTaskMessage implements Serializable {

    /** ID of the parent {@code JobDefinition} row in the database. */
    private UUID jobId;

    /** Human-readable job name (for log messages; avoids extra DB lookups). */
    private String jobName;

    /** Fully-qualified target URL that the Worker must invoke. */
    private String targetUrl;

    /** HTTP verb (GET, POST, PUT, DELETE, PATCH). */
    private String httpMethod;

    /**
     * The exact UTC instant when this job was scheduled to fire.
     * Used by the Worker to calculate scheduling drift (delay_ms).
     */
    private Instant scheduledTime;
}
