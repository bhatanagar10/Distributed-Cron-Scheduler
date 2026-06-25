package org.cron.distributed_cron_scheduler.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.cron.distributed_cron_scheduler.domain.JobDefinition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Manages the Redis ZSET used as the scheduling index for all enabled cron jobs.
 *
 * <h3>ZSET structure</h3>
 * <pre>
 *   Key:    {@code cron:schedule}  (configurable via {@code scheduler.redis-zset-key})
 *   Score:  next execution time as Unix epoch milliseconds
 *   Member: job UUID string
 * </pre>
 *
 * <p>The ZSET score is the next-run epoch-ms. A {@code ZPOPMIN} range query
 * atomically retrieves and removes all members whose score (next-run) is ≤ now,
 * preventing duplicate dispatches across multiple Scheduler instances.
 *
 * <p>PostgreSQL remains the <strong>source of truth</strong> for job definitions.
 * Redis is the fast scheduling index only.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RedisScheduleService {

    private final StringRedisTemplate redisTemplate;

    @Value("${scheduler.redis-zset-key:cron:schedule}")
    private String zsetKey;

    // ── Write operations ─────────────────────────────────────────────────────

    /**
     * Adds or updates a job's next-run slot in the ZSET.
     * If the job already exists its score is overwritten (ZADD XX behaviour).
     *
     * @param jobId     the job's UUID as a string
     * @param epochMs   the next execution time as Unix epoch milliseconds
     */
    public void scheduleJob(String jobId, long epochMs) {
        redisTemplate.opsForZSet().add(zsetKey, jobId, (double) epochMs);
        log.debug("ZADD {}  score={}  member={}", zsetKey, epochMs, jobId);
    }

    /**
     * Removes a job from the ZSET entirely (call on job deletion or disable).
     *
     * @param jobId the job's UUID as a string
     */
    public void removeJob(String jobId) {
        redisTemplate.opsForZSet().remove(zsetKey, (Object) jobId);
        log.debug("ZREM {}  member={}", zsetKey, jobId);
    }

    // ── Poll operation ───────────────────────────────────────────────────────

    /**
     * Atomically pops all jobs whose next-run score is ≤ {@code nowEpochMs}.
     *
     * <p>Uses {@code ZRANGEBYSCORE} + {@code ZREM} wrapped in a Redis Lua script
     * to guarantee atomicity: even with multiple Scheduler pods running concurrently,
     * each job ID is returned by exactly one pod.
     *
     * @param nowEpochMs current time as Unix epoch milliseconds
     * @return set of job ID strings that are due for execution (may be empty)
     */
    @SuppressWarnings("unchecked")
    public Set<String> popDueJobs(long nowEpochMs) {
        // Lua script: atomically range-query then delete in one server-side operation
        String luaScript = """
                local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1])
                if #jobs > 0 then
                    redis.call('ZREM', KEYS[1], unpack(jobs))
                end
                return jobs
                """;

        List<String> result = (List<String>) redisTemplate.execute(
                new DefaultRedisScript<>(luaScript, List.class),
                Collections.singletonList(zsetKey),
                String.valueOf(nowEpochMs)
        );

        if (result == null || result.isEmpty()) {
            return Collections.emptySet();
        }

        log.debug("ZPOPMIN(lua) {} ≤ {}ms → {} jobs", zsetKey, nowEpochMs, result.size());
        return new LinkedHashSet<>(result);
    }

    // ── Startup rebuild ──────────────────────────────────────────────────────

    /**
     * Rebuilds the entire ZSET from the supplied list of enabled job definitions.
     *
     * <p>Called by {@code SchedulerStartupListener} on {@code ApplicationReadyEvent}.
     * This is idempotent — running it multiple times does not create duplicates
     * (ZADD overwrites existing members).
     *
     * <p>Only jobs that have a future {@code nextExecutionTime} are added;
     * if a job's cached next-run is in the past, the next slot is NOT computed here —
     * the first poll cycle will handle it.
     *
     * @param jobs list of all enabled {@link JobDefinition}s from PostgreSQL
     */
    public void rebuildFromDatabase(List<JobDefinition> jobs) {
        if (jobs.isEmpty()) {
            log.info("No enabled jobs found in Postgres — Redis ZSET will be empty.");
            return;
        }

        Set<ZSetOperations.TypedTuple<String>> tuples = new java.util.HashSet<>();
        for (JobDefinition job : jobs) {
            if (job.getNextExecutionTime() != null) {
                tuples.add(ZSetOperations.TypedTuple.of(
                        job.getId().toString(),
                        (double) job.getNextExecutionTime().toEpochMilli()
                ));
            }
        }

        redisTemplate.opsForZSet().add(zsetKey, tuples);
        log.info("Redis ZSET '{}' rebuilt with {} job(s).", zsetKey, tuples.size());
    }
}
