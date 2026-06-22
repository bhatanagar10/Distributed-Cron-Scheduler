package org.cron.distributed_cron_scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Distributed Cron Scheduler.
 *
 * <h3>Active annotations</h3>
 * <ul>
 *   <li>{@link SpringBootApplication} — component scan, auto-configuration, property binding.</li>
 *   <li>{@link EnableScheduling}      — activates Spring's {@code @Scheduled} task executor,
 *       required by {@code SchedulerService#pollAndDispatch()}.</li>
 * </ul>
 *
 * <h3>Startup pre-requisites</h3>
 * Before starting the application, ensure the following services are reachable:
 * <ul>
 *   <li>PostgreSQL at {@code localhost:5432/crondb} (or override via environment variables)</li>
 *   <li>RabbitMQ at {@code localhost:5672} (or override via environment variables)</li>
 * </ul>
 * Run {@code docker compose up -d} from the project root to start both with Docker.
 */
@SpringBootApplication
@EnableScheduling
public class DistributedCronSchedulerApplication {

    @jakarta.annotation.PostConstruct
    public void init() {
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Kolkata"));
    }

    public static void main(String[] args) {
        SpringApplication.run(DistributedCronSchedulerApplication.class, args);
    }
}
