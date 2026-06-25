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
 * <p>Each job is defined by a standard 5-field Linux cron expression (IST timezone):
 * <pre>
 *   ┌───── minute  (0–59)
 *   │ ┌─── hour    (0–23)
 *   │ │ ┌─ day-of-month (1–31)
 *   │ │ │ ┌ month  (1–12)
 *   │ │ │ │ ┌ day-of-week (0–7, 0 and 7 = Sunday)
 *   * * * * *
 * </pre>
 * Examples: {@code "*{@literal /}5 * * * *"} = every 5 minutes,
 *           {@code "0 10 * * 1-5"} = 10:00 AM IST on weekdays.
 *
 * <h3>Concurrency safety</h3>
 * The Redis ZSET ({@code cron:schedule}) is the scheduling index. The
 * {@link #nextExecutionTime} column in Postgres is kept as a fallback cache
 * used by the startup listener to rebuild the ZSET after a Redis restart.
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
     * Standard 5-field Linux cron expression interpreted in IST (Asia/Kolkata).
     * Examples:
     * <ul>
     *   <li>{@code "*\/5 * * * *"} — every 5 minutes</li>
     *   <li>{@code "0 10 * * 1-5"} — 10:00 AM IST, Monday–Friday</li>
     *   <li>{@code "30 9 1 * *"} — 09:30 IST on the 1st of every month</li>
     * </ul>
     */
    @Column(name = "cron_expression", nullable = false, length = 100)
    private String cronExpression;

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
