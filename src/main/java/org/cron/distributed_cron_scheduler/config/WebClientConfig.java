package org.cron.distributed_cron_scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Configuration for the reactive {@link WebClient} used by the Execution Engine (Workers)
 * to invoke target HTTP endpoints.
 *
 * <p>A single shared instance is injected into {@code JobExecutionWorker}. Per-request
 * timeouts are applied dynamically using Reactor's {@code .timeout(Duration)} operator
 * rather than being baked into the client, so the same bean works for both fast and slow
 * target APIs.
 *
 * <p>The in-memory buffer is capped at <b>10 MB</b> to protect the JVM heap when target
 * endpoints return large response bodies.
 */
@Configuration
public class WebClientConfig {

    private static final int MAX_IN_MEMORY_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(MAX_IN_MEMORY_SIZE_BYTES))
                .build();
    }
}
