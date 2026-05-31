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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import org.fireflyframework.cache.core.CacheHealth;
import org.fireflyframework.cache.core.CacheType;
import org.fireflyframework.cache.serialization.CacheSerializer;
import org.fireflyframework.cache.serialization.JsonCacheSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for {@link PostgresCacheAdapter} against a real PostgreSQL
 * instance provisioned via Testcontainers.
 */
@Testcontainers
class PostgresCacheAdapterIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("cache")
            .withUsername("cache")
            .withPassword("cache");

    private static final String SCHEMA = "public";
    private static final String TABLE = "firefly_cache_entries";

    private static ConnectionPool pool;
    private PostgresCacheAdapter adapter;

    @BeforeAll
    static void startPool() {
        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host(POSTGRES.getHost())
                .port(POSTGRES.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                .database(POSTGRES.getDatabaseName())
                .username(POSTGRES.getUsername())
                .password(POSTGRES.getPassword())
                .schema(SCHEMA)
                .build();
        PostgresqlConnectionFactory delegate = new PostgresqlConnectionFactory(config);
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(delegate)
                .maxSize(5)
                .initialSize(1)
                .build();
        pool = new ConnectionPool(poolConfig);
        createTable();
    }

    @AfterAll
    static void closePool() {
        if (pool != null) {
            pool.dispose();
        }
    }

    private static void createTable() {
        String qualified = "\"" + SCHEMA + "\".\"" + TABLE + "\"";
        String createTable = "CREATE TABLE IF NOT EXISTS " + qualified + " ("
                + "cache_name VARCHAR(255) NOT NULL, "
                + "cache_key VARCHAR(1024) NOT NULL, "
                + "value BYTEA NOT NULL, "
                + "expires_at TIMESTAMPTZ, "
                + "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "PRIMARY KEY (cache_name, cache_key))";
        String createIndex = "CREATE INDEX IF NOT EXISTS " + TABLE + "_expires_idx "
                + "ON " + qualified + " (expires_at) WHERE expires_at IS NOT NULL";
        Mono.usingWhen(
                        Mono.from(pool.create()),
                        conn -> Mono.from(conn.createStatement(createTable).execute())
                                .flatMap(r -> Mono.from(r.getRowsUpdated())).then()
                                .then(Mono.from(conn.createStatement(createIndex).execute())
                                        .flatMap(r -> Mono.from(r.getRowsUpdated())).then()),
                        Connection::close)
                .block(Duration.ofSeconds(30));
    }

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        CacheSerializer serializer = new JsonCacheSerializer(objectMapper);
        PostgresCacheConfig config = PostgresCacheConfig.builder()
                .cacheName("itest")
                .keyPrefix("firefly:cache")
                .defaultTtl(Duration.ofMinutes(30))
                .schema(SCHEMA)
                .cacheTable(TABLE)
                .build();
        adapter = new PostgresCacheAdapter("itest", pool, config, serializer);
        // Ensure a clean cache for each test.
        adapter.clear().block(Duration.ofSeconds(10));
    }

    @Test
    void putThenGetRoundTrip() {
        StepVerifier.create(adapter.put("user:1", "Alice"))
                .verifyComplete();

        StepVerifier.create(adapter.<String, String>get("user:1", String.class))
                .assertNext(opt -> assertThat(opt).contains("Alice"))
                .verifyComplete();
    }

    @Test
    void getMissReturnsEmpty() {
        StepVerifier.create(adapter.<String, String>get("absent", String.class))
                .assertNext(opt -> assertThat(opt).isEmpty())
                .verifyComplete();
    }

    @Test
    void putRoundTripWithComplexObject() {
        SampleDto dto = new SampleDto("widget", 42);
        StepVerifier.create(adapter.put("dto:1", dto)).verifyComplete();

        StepVerifier.create(adapter.<String, SampleDto>get("dto:1", SampleDto.class))
                .assertNext(opt -> {
                    assertThat(opt).isPresent();
                    assertThat(opt.get().getName()).isEqualTo("widget");
                    assertThat(opt.get().getCount()).isEqualTo(42);
                })
                .verifyComplete();
    }

    @Test
    void ttlExpiryRemovesEntry() {
        adapter.put("ephemeral", "soon-gone", Duration.ofSeconds(1)).block(Duration.ofSeconds(5));

        // Immediately present.
        Optional<String> present = adapter.<String, String>get("ephemeral", String.class)
                .block(Duration.ofSeconds(5));
        assertThat(present).contains("soon-gone");

        // After TTL elapses the entry is filtered out on read.
        await().atMost(5, TimeUnit.SECONDS).pollInterval(250, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Optional<String> expired = adapter.<String, String>get("ephemeral", String.class)
                    .block(Duration.ofSeconds(5));
            assertThat(expired).isEmpty();
        });
    }

    @Test
    void evictRemovesEntry() {
        adapter.put("toEvict", "value").block(Duration.ofSeconds(5));

        StepVerifier.create(adapter.evict("toEvict"))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(adapter.exists("toEvict"))
                .expectNext(false)
                .verifyComplete();

        // Evicting an absent key returns false.
        StepVerifier.create(adapter.evict("toEvict"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void existsReflectsPresence() {
        StepVerifier.create(adapter.exists("k"))
                .expectNext(false)
                .verifyComplete();

        adapter.put("k", "v").block(Duration.ofSeconds(5));

        StepVerifier.create(adapter.exists("k"))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void sizeCountsLiveEntries() {
        adapter.put("a", "1").block(Duration.ofSeconds(5));
        adapter.put("b", "2").block(Duration.ofSeconds(5));
        adapter.put("c", "3").block(Duration.ofSeconds(5));

        StepVerifier.create(adapter.size())
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void clearRemovesAllEntriesForCache() {
        adapter.put("x", "1").block(Duration.ofSeconds(5));
        adapter.put("y", "2").block(Duration.ofSeconds(5));

        StepVerifier.create(adapter.clear()).verifyComplete();

        StepVerifier.create(adapter.size())
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void putIfAbsentOnlyStoresWhenMissing() {
        StepVerifier.create(adapter.putIfAbsent("once", "first"))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(adapter.putIfAbsent("once", "second"))
                .expectNext(false)
                .verifyComplete();

        StepVerifier.create(adapter.<String, String>get("once", String.class))
                .assertNext(opt -> assertThat(opt).contains("first"))
                .verifyComplete();
    }

    @Test
    void evictByPrefixDeletesMatchingKeys() {
        adapter.put("session:1", "a").block(Duration.ofSeconds(5));
        adapter.put("session:2", "b").block(Duration.ofSeconds(5));
        adapter.put("other:1", "c").block(Duration.ofSeconds(5));

        StepVerifier.create(adapter.evictByPrefix("session:"))
                .expectNext(2L)
                .verifyComplete();

        StepVerifier.create(adapter.exists("session:1"))
                .expectNext(false)
                .verifyComplete();
        StepVerifier.create(adapter.exists("other:1"))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void keysReturnsUnprefixedKeys() {
        adapter.put("alpha", "1").block(Duration.ofSeconds(5));
        adapter.put("beta", "2").block(Duration.ofSeconds(5));

        Set<String> keys = adapter.<String>keys().block(Duration.ofSeconds(5));
        assertThat(keys).containsExactlyInAnyOrder("alpha", "beta");
    }

    @Test
    void getHealthReportsUp() {
        CacheHealth health = adapter.getHealth().block(Duration.ofSeconds(10));
        assertThat(health).isNotNull();
        assertThat(health.getCacheType()).isEqualTo(CacheType.POSTGRES);
        assertThat(health.isAvailable()).isTrue();
        assertThat(health.getStatus()).isEqualTo("UP");
    }

    @Test
    void getCacheTypeIsPostgres() {
        assertThat(adapter.getCacheType()).isEqualTo(CacheType.POSTGRES);
        assertThat(adapter.getCacheName()).isEqualTo("itest");
        assertThat(adapter.isAvailable()).isTrue();
    }

    @Test
    void statsTrackHitsAndMisses() {
        adapter.put("s", "v").block(Duration.ofSeconds(5));
        adapter.<String, String>get("s", String.class).block(Duration.ofSeconds(5));
        adapter.<String, String>get("missing", String.class).block(Duration.ofSeconds(5));

        var stats = adapter.getStats().block(Duration.ofSeconds(5));
        assertThat(stats).isNotNull();
        assertThat(stats.getHitCount()).isGreaterThanOrEqualTo(1L);
        assertThat(stats.getMissCount()).isGreaterThanOrEqualTo(1L);
        assertThat(stats.getCacheType()).isEqualTo(CacheType.POSTGRES);
    }

    /**
     * Simple serializable DTO used to verify JSON round-trips through BYTEA.
     */
    public static class SampleDto {
        private String name;
        private int count;

        public SampleDto() {
        }

        public SampleDto(String name, int count) {
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
