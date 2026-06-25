package org.cron.distributed_cron_scheduler.service;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Optional;

/**
 * Utility service for parsing and evaluating standard 5-field Linux (UNIX) cron expressions.
 *
 * <p>All execution times are computed in <strong>IST (Asia/Kolkata, UTC+5:30)</strong>.
 *
 * <h3>Supported expression format (5 fields)</h3>
 * <pre>
 *   ┌───── minute       (0–59)
 *   │ ┌─── hour         (0–23)
 *   │ │ ┌─ day-of-month (1–31)
 *   │ │ │ ┌ month       (1–12)
 *   │ │ │ │ ┌ day-of-week (0–7, both 0 and 7 = Sunday)
 *   * * * * *
 * </pre>
 *
 * <h3>Examples</h3>
 * <ul>
 *   <li>{@code "* /5 * * * *"}  — every 5 minutes (remove the space)</li>
 *   <li>{@code "0 10 * * 1-5"} — 10:00 AM IST, Monday–Friday</li>
 *   <li>{@code "30 9 1 * *"}   — 09:30 IST on the 1st of every month</li>
 * </ul>
 */
@Service
@Slf4j
public class CronService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    /**
     * UNIX-style cron definition (5 fields, no seconds).
     * This matches the standard Linux crontab format.
     */
    private static final CronDefinition CRON_DEFINITION =
            CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);

    private final CronParser     parser     = new CronParser(CRON_DEFINITION);
    private final CronDescriptor descriptor = CronDescriptor.instance(Locale.ENGLISH);

    /**
     * Computes the next execution instant strictly after {@code from}, interpreted in IST.
     *
     * @param expression a valid 5-field cron expression
     * @param from       the reference instant (typically {@code Instant.now()})
     * @return the next fire instant in UTC
     * @throws IllegalArgumentException if the expression is invalid or no next time exists
     */
    public Instant nextExecution(String expression, Instant from) {
        Cron cron = parser.parse(expression);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);

        ZonedDateTime fromIst = ZonedDateTime.ofInstant(from, IST);

        Optional<ZonedDateTime> next = executionTime.nextExecution(fromIst);
        return next
                .map(ZonedDateTime::toInstant)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Cron expression '" + expression + "' has no future execution after " + from));
    }

    /**
     * Validates whether the given string is a parseable 5-field cron expression.
     *
     * @param expression the string to validate
     * @return {@code true} if valid, {@code false} otherwise
     */
    public boolean isValid(String expression) {
        if (expression == null || expression.isBlank()) return false;
        try {
            parser.parse(expression).validate();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns a human-readable English description of the cron expression.
     * Useful for log messages and API responses.
     *
     * @param expression a valid 5-field cron expression
     * @return e.g. "every 5 minutes" or "at 10:00 AM, Monday through Friday"
     */
    public String describe(String expression) {
        try {
            return descriptor.describe(parser.parse(expression));
        } catch (Exception e) {
            return expression;
        }
    }
}
