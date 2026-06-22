package org.cron.distributed_cron_scheduler.domain;

import jakarta.persistence.*;
import lombok.*;
import org.cron.distributed_cron_scheduler.enums.ExecutionStatus;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity representing a row in the {@code job_execution_history} table.
 *
 * <p>This table is append-only — records are never updated after insertion.
 * Every field except {@link #delayMs} (which is a PostgreSQL generated column)
 * is supplied by the Worker immediately after an execution attempt completes.
 *
 * <p>{@link #delayMs} is computed by the database as:
 * <pre>
 *   EXTRACT(EPOCH FROM (actual_start_time - scheduled_time)) * 1000
 * </pre>
 * The Java field is mapped as {@code insertable=false, updatable=false} so that
 * Hibernate never tries to write to it.
 */
@Entity
@Table(name = "job_execution_history")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobExecutionHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID id;

    /**
     * The parent job that triggered this execution attempt.
     * Fetched lazily to avoid N+1 issues in list queries.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false, foreignKey = @ForeignKey(name = "fk_jeh_job"))
    private JobDefinition job;

    /**
     * The UTC timestamp at which this job was supposed to fire (from {@code JobTaskMessage}).
     */
    @Column(name = "scheduled_time", nullable = false)
    private Instant scheduledTime;

    /**
     * The UTC timestamp captured by the Worker the moment it started executing the HTTP call.
     */
    @Column(name = "actual_start_time", nullable = false)
    private Instant actualStartTime;

    /**
     * Scheduling drift in milliseconds — computed entirely by PostgreSQL.
     * A positive value means the job started later than scheduled (normal drift).
     * A large positive value indicates system overload or queue congestion.
     */
    @Column(name = "delay_ms", insertable = false, updatable = false)
    private Long delayMs;

    /**
     * Final outcome of this execution attempt.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ExecutionStatus status;

    /**
     * HTTP status code returned by the target URL. {@code null} if the request
     * never reached the server (e.g. DNS failure, timeout before connection).
     */
    @Column(name = "http_status_code")
    private Integer httpStatusCode;

    /**
     * The first 10KB of the HTTP response body (or error message on failure).
     */
    @Column(name = "response_payload", columnDefinition = "TEXT")
    private String responsePayload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @PrePersist
    protected void onCreate() {
        if (this.createdAt == null) {
            this.createdAt = Instant.now();
        }
    }
}
