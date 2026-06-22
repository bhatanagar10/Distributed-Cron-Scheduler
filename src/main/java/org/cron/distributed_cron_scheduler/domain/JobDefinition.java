package org.cron.distributed_cron_scheduler.domain;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity representing a row in the {@code job_definitions} table.
 *
 * <h3>Scheduling model</h3>
 * <p>Each job is defined by an <em>anchor time</em> ({@link #startHour}:{@link #startMinute} IST)
 * and a recurrence {@link #intervalMinutes}.  All future execution slots are computed
 * deterministically from the anchor:
 * <pre>
 *   slot(n) = anchor + n × intervalMinutes
 * </pre>
 * This means scheduling drift (e.g. broker lag) is <em>never</em> accumulated —
 * the next slot is always the smallest {@code slot(n)} that is strictly after {@code now}.
 *
 * <h3>Concurrency safety</h3>
 * The {@link #nextExecutionTime} column is the primary field used by the Scheduler's locking
 * query ({@code SELECT FOR UPDATE SKIP LOCKED}) to identify due jobs and prevent duplicate
 * dispatch across multiple Manager instances.
 */
@Entity
@Table(name = "job_definitions")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString()
public class JobDefinition {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @JdbcTypeCode(SqlTypes.UUID)
    private UUID id;

    /**
     * Human-readable unique identifier for the job (e.g. "payment-health-check").
     */
    @Column(nullable = false, unique = true, length = 255)
    private String name;

    /**
     * Fully-qualified URL that the Worker will invoke when this job fires.
     */
    @Column(name = "target_url", nullable = false, columnDefinition = "TEXT")
    private String targetUrl;

    /**
     * HTTP verb to use when calling {@link #targetUrl} (GET, POST, PUT, DELETE, PATCH).
     */
    @Column(name = "http_method", nullable = false, length = 10)
    private String httpMethod;

    /**
     * IST hour component (0–23) of the daily anchor time.
     * Together with {@link #startMinute} this defines the first execution slot of every day.
     */
    @Column(name = "start_hour", nullable = false)
    private Integer startHour;

    /**
     * IST minute component (0–59) of the daily anchor time.
     */
    @Column(name = "start_minute", nullable = false)
    private Integer startMinute;

    /**
     * Recurrence interval in minutes (1–1440).
     * Subsequent slots are anchor + N × intervalMinutes.
     */
    @Column(name = "interval_minutes", nullable = false)
    private Integer intervalMinutes;

    /**
     * When {@code false}, the Scheduler's poller will skip this job entirely.
     */
    @Column(nullable = false)
    @Builder.Default
    private Boolean enabled = Boolean.TRUE;

    /**
     * UTC timestamp of the next scheduled invocation.
     * Updated atomically by the Scheduler inside a {@code SELECT FOR UPDATE SKIP LOCKED}
     * transaction after each successful dispatch to RabbitMQ.
     */
    @Column(name = "next_execution_time", nullable = false)
    private Instant nextExecutionTime;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    // ── Lifecycle callbacks ──────────────────────────────────────────────────

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = Instant.now();
    }
}
