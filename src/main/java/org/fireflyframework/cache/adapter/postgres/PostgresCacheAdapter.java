/*
 * Copyright 2024-2026 Firefly Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fireflyframework.cache.adapter.postgres;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.fireflyframework.cache.core.CacheAdapter;
import org.fireflyframework.cache.core.CacheHealth;
import org.fireflyframework.cache.core.CacheStats;
import org.fireflyframework.cache.core.CacheType;
import org.fireflyframework.cache.serialization.CacheSerializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PostgreSQL-backed reactive cache adapter implemented over R2DBC.
 * <p>
 * Cache entries are stored in a single relational table keyed by
 * {@code (cache_name, cache_key)}. Values are serialized with a
 * {@link CacheSerializer} into a JSON {@link String}, encoded as UTF-8 bytes and
 * persisted in a {@code BYTEA} column. Expiry is enforced both lazily on read
 * (rows whose {@code expires_at} is in the past are treated as absent) and at
 * write time ({@code expires_at = NOW() + ttl}).
 * <p>
 * All operations run reactively against a Spring-managed
 * {@link ConnectionFactory}; connections are acquired and released per operation
 * via {@link Mono#usingWhen(Publisher, java.util.function.Function, java.util.function.Function)}.
 */
public class PostgresCacheAdapter implements CacheAdapter {

    private static final Logger log = LoggerFactory.getLogger(PostgresCacheAdapter.class);

    private final String cacheName;
    private final String keyPrefix;
    private final ConnectionFactory connectionFactory;
    private final PostgresCacheConfig config;
    private final CacheSerializer serializer;
    private final String qualifiedTable;

    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong putCount = new AtomicLong();
    private final AtomicLong evictionCount = new AtomicLong();
    private final AtomicLong totalLoadTime = new AtomicLong();

    public PostgresCacheAdapter(String cacheName,
                                ConnectionFactory connectionFactory,
                                PostgresCacheConfig config,
                                CacheSerializer serializer) {
        this.cacheName = cacheName;
        this.connectionFactory = connectionFactory;
        this.config = config;
        this.serializer = serializer;
        this.keyPrefix = buildKeyPrefix(cacheName, config.getKeyPrefix());
        String schema = config.getSchema() != null ? config.getSchema() : "public";
        String table = config.getCacheTable() != null ? config.getCacheTable() : "firefly_cache_entries";
        this.qualifiedTable = "\"" + schema + "\".\"" + table + "\"";
    }

    private String buildKeyPrefix(String cacheName, String configPrefix) {
        if (configPrefix != null && !configPrefix.trim().isEmpty()) {
            return configPrefix + ":" + cacheName + ":";
        }
        return "cache:" + cacheName + ":";
    }

    private String buildKey(Object key) {
        return keyPrefix + key.toString();
    }

    private String extractOriginalKey(String cacheKey) {
        if (cacheKey.startsWith(keyPrefix)) {
            return cacheKey.substring(keyPrefix.length());
        }
        return cacheKey;
    }

    /**
     * Serializes a value to UTF-8 JSON bytes for storage in the {@code BYTEA} column.
     */
    private byte[] toBytes(Object value) {
        Object serialized = serializer.serialize(value);
        if (serialized instanceof byte[] bytes) {
            return bytes;
        }
        if (serialized instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        // Primitives/Numbers are returned as-is by JsonCacheSerializer; stringify them.
        return String.valueOf(serialized).getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private <V> V fromBytes(byte[] bytes, Class<V> valueType) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return serializer.deserialize(json, valueType);
    }

    private byte[] readBytes(Readable row, String column) {
        Object raw = row.get(column);
        if (raw == null) {
            return null;
        }
        if (raw instanceof byte[] bytes) {
            return bytes;
        }
        if (raw instanceof java.nio.ByteBuffer buf) {
            byte[] out = new byte[buf.remaining()];
            buf.get(out);
            return out;
        }
        return raw.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Runs the supplied work against a freshly acquired connection, ensuring the
     * connection is released afterward.
     */
    private <T> Mono<T> withConnection(java.util.function.Function<Connection, Mono<T>> work) {
        return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                work::apply,
                Connection::close,
                (conn, err) -> Mono.from(conn.close()).then(Mono.error(err)),
                Connection::close);
    }

    @Override
    public <K, V> Mono<Optional<V>> get(K key) {
        return get(key, (Class<V>) (Class<?>) Object.class);
    }

    @Override
    public <K, V> Mono<Optional<V>> get(K key, Class<V> valueType) {
        String fullKey = buildKey(key);
        long start = System.nanoTime();
        requestCount.incrementAndGet();
        String sql = "SELECT value FROM " + qualifiedTable
                + " WHERE cache_name = $1 AND cache_key = $2"
                + " AND (expires_at IS NULL OR expires_at > NOW())";
        return withConnection(conn -> {
            Statement st = conn.createStatement(sql)
                    .bind("$1", cacheName)
                    .bind("$2", fullKey);
            return Flux.from(st.execute())
                    .flatMap(result -> result.map((row, meta) -> readBytes(row, "value")))
                    .next();
        }).map(bytes -> {
            try {
                V value = fromBytes(bytes, valueType);
                hitCount.incrementAndGet();
                totalLoadTime.addAndGet(System.nanoTime() - start);
                return Optional.ofNullable(value);
            } catch (Exception e) {
                log.warn("Failed to deserialize cached value for key '{}' in cache '{}': {}",
                        key, cacheName, e.getMessage());
                missCount.incrementAndGet();
                return Optional.<V>empty();
            }
        }).switchIfEmpty(Mono.fromSupplier(() -> {
            missCount.incrementAndGet();
            return Optional.<V>empty();
        })).onErrorResume(e -> {
            log.warn("Error reading key '{}' from cache '{}': {}", key, cacheName, e.getMessage());
            missCount.incrementAndGet();
            return Mono.just(Optional.<V>empty());
        });
    }

    @Override
    public <K, V> Mono<Void> put(K key, V value) {
        return put(key, value, config.getDefaultTtl());
    }

    @Override
    public <K, V> Mono<Void> put(K key, V value, Duration ttl) {
        String fullKey = buildKey(key);
        return Mono.fromCallable(() -> toBytes(value))
                .flatMap(bytes -> {
                    boolean hasTtl = ttl != null && !ttl.isZero() && !ttl.isNegative();
                    return withConnection(conn -> {
                        Statement st;
                        if (hasTtl) {
                            String insertSql = "INSERT INTO " + qualifiedTable
                                    + " (cache_name, cache_key, value, expires_at, created_at)"
                                    + " VALUES ($1, $2, $3, NOW() + ($4 || ' milliseconds')::interval, NOW())"
                                    + " ON CONFLICT (cache_name, cache_key) DO UPDATE"
                                    + " SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at, created_at = NOW()";
                            st = conn.createStatement(insertSql)
                                    .bind("$1", cacheName)
                                    .bind("$2", fullKey)
                                    .bind("$3", bytes)
                                    .bind("$4", String.valueOf(ttl.toMillis()));
                        } else {
                            String insertSql = "INSERT INTO " + qualifiedTable
                                    + " (cache_name, cache_key, value, expires_at, created_at)"
                                    + " VALUES ($1, $2, $3, NULL, NOW())"
                                    + " ON CONFLICT (cache_name, cache_key) DO UPDATE"
                                    + " SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at, created_at = NOW()";
                            st = conn.createStatement(insertSql)
                                    .bind("$1", cacheName)
                                    .bind("$2", fullKey)
                                    .bind("$3", bytes);
                        }
                        return rowsUpdated(st).doOnNext(n -> putCount.incrementAndGet()).then();
                    });
                });
    }

    @Override
    public <K, V> Mono<Boolean> putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, config.getDefaultTtl());
    }

    @Override
    public <K, V> Mono<Boolean> putIfAbsent(K key, V value, Duration ttl) {
        String fullKey = buildKey(key);
        return Mono.fromCallable(() -> toBytes(value))
                .flatMap(bytes -> {
                    boolean hasTtl = ttl != null && !ttl.isZero() && !ttl.isNegative();
                    String sql;
                    if (hasTtl) {
                        sql = "INSERT INTO " + qualifiedTable
                                + " (cache_name, cache_key, value, expires_at, created_at)"
                                + " VALUES ($1, $2, $3, NOW() + ($4 || ' milliseconds')::interval, NOW())"
                                + " ON CONFLICT (cache_name, cache_key) DO NOTHING";
                    } else {
                        sql = "INSERT INTO " + qualifiedTable
                                + " (cache_name, cache_key, value, expires_at, created_at)"
                                + " VALUES ($1, $2, $3, NULL, NOW())"
                                + " ON CONFLICT (cache_name, cache_key) DO NOTHING";
                    }
                    return withConnection(conn -> {
                        Statement st = conn.createStatement(sql)
                                .bind("$1", cacheName)
                                .bind("$2", fullKey)
                                .bind("$3", bytes);
                        if (hasTtl) {
                            st = st.bind("$4", String.valueOf(ttl.toMillis()));
                        }
                        return rowsUpdated(st);
                    }).map(n -> {
                        if (n > 0) {
                            putCount.incrementAndGet();
                            return true;
                        }
                        return false;
                    });
                });
    }

    @Override
    public <K> Mono<Boolean> evict(K key) {
        String fullKey = buildKey(key);
        String sql = "DELETE FROM " + qualifiedTable + " WHERE cache_name = $1 AND cache_key = $2";
        return withConnection(conn -> rowsUpdated(conn.createStatement(sql)
                .bind("$1", cacheName)
                .bind("$2", fullKey)))
                .map(n -> {
                    boolean removed = n > 0;
                    if (removed) {
                        evictionCount.incrementAndGet();
                    }
                    return removed;
                })
                .onErrorResume(e -> {
                    log.warn("Error evicting key '{}' from cache '{}': {}", key, cacheName, e.getMessage());
                    return Mono.just(false);
                });
    }

    @Override
    public Mono<Void> clear() {
        String sql = "DELETE FROM " + qualifiedTable + " WHERE cache_name = $1";
        return withConnection(conn -> rowsUpdated(conn.createStatement(sql).bind("$1", cacheName)))
                .doOnNext(evictionCount::addAndGet)
                .then();
    }

    @Override
    public Mono<Long> evictByPrefix(String keyPrefixToMatch) {
        String fullPrefix = buildKey(keyPrefixToMatch);
        String sql = "DELETE FROM " + qualifiedTable
                + " WHERE cache_name = $1 AND cache_key LIKE $2 || '%'";
        return withConnection(conn -> rowsUpdated(conn.createStatement(sql)
                .bind("$1", cacheName)
                .bind("$2", fullPrefix)))
                .doOnNext(evictionCount::addAndGet);
    }

    @Override
    public <K> Mono<Boolean> exists(K key) {
        String fullKey = buildKey(key);
        String sql = "SELECT 1 FROM " + qualifiedTable
                + " WHERE cache_name = $1 AND cache_key = $2"
                + " AND (expires_at IS NULL OR expires_at > NOW())";
        return withConnection(conn -> Flux.from(conn.createStatement(sql)
                .bind("$1", cacheName)
                .bind("$2", fullKey)
                .execute())
                .flatMap(result -> result.map((row, meta) -> Boolean.TRUE))
                .next())
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.warn("Error checking existence of key '{}' in cache '{}': {}",
                            key, cacheName, e.getMessage());
                    return Mono.just(false);
                });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> Mono<Set<K>> keys() {
        String sql = "SELECT cache_key FROM " + qualifiedTable
                + " WHERE cache_name = $1 AND (expires_at IS NULL OR expires_at > NOW())";
        return withConnection(conn -> Flux.from(conn.createStatement(sql)
                .bind("$1", cacheName)
                .execute())
                .flatMap(result -> result.map((row, meta) -> (String) row.get("cache_key")))
                .map(this::extractOriginalKey)
                .map(k -> (K) k)
                .collect(HashSet::new, Set::add))
                .map(set -> (Set<K>) set);
    }

    @Override
    public Mono<Long> size() {
        String sql = "SELECT COUNT(*) AS cnt FROM " + qualifiedTable
                + " WHERE cache_name = $1 AND (expires_at IS NULL OR expires_at > NOW())";
        return withConnection(conn -> Flux.from(conn.createStatement(sql)
                .bind("$1", cacheName)
                .execute())
                .flatMap(result -> result.map((row, meta) -> {
                    Number n = (Number) row.get("cnt");
                    return n == null ? 0L : n.longValue();
                }))
                .next())
                .defaultIfEmpty(0L);
    }

    @Override
    public Mono<CacheStats> getStats() {
        return size().map(entryCount -> CacheStats.builder()
                        .requestCount(requestCount.get())
                        .hitCount(hitCount.get())
                        .missCount(missCount.get())
                        .loadCount(putCount.get())
                        .evictionCount(evictionCount.get())
                        .entryCount(entryCount)
                        .averageLoadTime(hitCount.get() > 0 ? (double) totalLoadTime.get() / hitCount.get() : 0.0)
                        .estimatedSize(0)
                        .capturedAt(Instant.now())
                        .cacheType(CacheType.POSTGRES)
                        .cacheName(cacheName)
                        .build())
                .onErrorReturn(CacheStats.empty(CacheType.POSTGRES, cacheName));
    }

    @Override
    public CacheType getCacheType() {
        return CacheType.POSTGRES;
    }

    @Override
    public String getCacheName() {
        return cacheName;
    }

    @Override
    public boolean isAvailable() {
        return connectionFactory != null;
    }

    @Override
    public Mono<CacheHealth> getHealth() {
        long start = System.currentTimeMillis();
        return withConnection(conn -> Mono.from(conn.validate(ValidationDepth.REMOTE)))
                .map(valid -> {
                    long responseTime = System.currentTimeMillis() - start;
                    if (Boolean.TRUE.equals(valid)) {
                        return CacheHealth.builder()
                                .status("UP")
                                .cacheType(CacheType.POSTGRES)
                                .cacheName(cacheName)
                                .available(true)
                                .configured(true)
                                .responseTimeMs(responseTime)
                                .lastSuccessfulOperation(Instant.now())
                                .details(Map.of(
                                        "schema", config.getSchema(),
                                        "table", config.getCacheTable(),
                                        "key_prefix", keyPrefix))
                                .build();
                    }
                    return CacheHealth.unhealthy(CacheType.POSTGRES, cacheName,
                            "Connection validation returned false", null);
                })
                .onErrorResume(e -> Mono.just(CacheHealth.unhealthy(CacheType.POSTGRES, cacheName,
                        "Health check failed: " + e.getMessage(), e)));
    }

    @Override
    public void close() {
        // The ConnectionFactory/ConnectionPool is Spring-managed (disposed via the
        // bean destroy method); nothing to close here.
        log.info("Closing PostgreSQL cache adapter '{}'", cacheName);
    }

    private Mono<Long> rowsUpdated(Statement statement) {
        return Flux.from(statement.execute())
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .map(Number::longValue)
                .reduce(0L, Long::sum);
    }
}
