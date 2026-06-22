package org.cron.distributed_cron_scheduler.messaging;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ topology and converter configuration.
 *
 * <h3>Topology</h3>
 * <pre>
 *   Producer (Scheduler)
 *       └─► [cron.exchange] ──(routing key: cron.job.execute)──► [cron.jobs queue]
 *                                                                       │
 *                                                                Consumer (Workers)
 * </pre>
 *
 * <ul>
 *   <li>The exchange is a <b>durable Direct Exchange</b> — survives broker restarts.</li>
 *   <li>The queue is <b>durable</b> — messages persist to disk even if RabbitMQ restarts.</li>
 *   <li>All messages are published with <b>persistent delivery mode</b> via the {@link RabbitTemplate}.</li>
 * </ul>
 */
@Configuration
public class RabbitMQConfig {

    public static final String QUEUE_NAME    = "cron.jobs";
    public static final String EXCHANGE_NAME = "cron.exchange";
    public static final String ROUTING_KEY   = "cron.job.execute";

    // ── Topology Beans ───────────────────────────────────────────────────────

    /**
     * Durable queue — messages survive a RabbitMQ broker restart.
     */
    @Bean
    public Queue cronJobsQueue() {
        return QueueBuilder
                .durable(QUEUE_NAME)
                .build();
    }

    /**
     * Durable Direct Exchange — routes messages by exact routing key.
     */
    @Bean
    public DirectExchange cronExchange() {
        return ExchangeBuilder
                .directExchange(EXCHANGE_NAME)
                .durable(true)
                .build();
    }

    /**
     * Binds the queue to the exchange using the routing key.
     */
    @Bean
    public Binding cronJobBinding(Queue cronJobsQueue, DirectExchange cronExchange) {
        return BindingBuilder
                .bind(cronJobsQueue)
                .to(cronExchange)
                .with(ROUTING_KEY);
    }

    // ── Serialization ────────────────────────────────────────────────────────

    /**
     * Jackson message converter using the default constructor of JacksonJsonMessageConverter.
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    /**
     * RabbitTemplate pre-configured with the JSON converter.
     * Publishes messages with PERSISTENT delivery mode by default.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        return template;
    }

    /**
     * Listener container factory used by {@code @RabbitListener} methods.
     * Concurrency and prefetch are configured in {@code application.yaml}.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter jsonMessageConverter) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter);
        return factory;
    }
}
