package org.cron.distributed_cron_scheduler.exception;

import java.util.UUID;

/**
 * Thrown when a {@code JobDefinition} with the requested ID does not exist in the database.
 * Mapped to HTTP 404 by {@link GlobalExceptionHandler}.
 */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID id) {
        super("Job not found with id: " + id);
    }
}
