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

package org.fireflyframework.cache.config;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.fireflyframework.cache.properties.CacheProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for the PostgreSQL R2DBC cache provider.
 * <p>
 * Creates a pooled {@link ConnectionFactory} dedicated to cache storage and,
 * when {@code firefly.cache.postgres.auto-create-schema} is {@code true},
 * provisions the cache table and a supporting expiry index. The resulting bean
 * (named {@code fireflyCachePostgresConnectionFactory}) is what the cache core
 * resolves reflectively into the provider context's
 * {@code r2dbcConnectionFactory}.
 * <p>
 * <strong>Configuration source:</strong> {@code firefly.cache.postgres.*} --
 * never {@code spring.r2dbc.*}.
 */
@AutoConfiguration(after = CacheAutoConfiguration.class)
@ConditionalOnClass(name = "io.r2dbc.spi.ConnectionFactory")
@ConditionalOnProperty(prefix = "firefly.cache.postgres", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CacheProperties.class)
@Slf4j
public class PostgresCacheAutoConfiguration {

    /**
     * Creates the pooled R2DBC {@link ConnectionFactory} the cache adapter uses.
     */
    @Bean(name = "fireflyCachePostgresConnectionFactory", destroyMethod = "dispose")
    @ConditionalOnMissingBean(name = "fireflyCachePostgresConnectionFactory")
    public ConnectionFactory fireflyCachePostgresConnectionFactory(CacheProperties properties) {
        CacheProperties.PostgresConfig cfg = properties.getPostgres();
        log.info("Creating PostgreSQL cache ConnectionFactory: host={}:{}, database={}, schema={}, table={}",
                cfg.getHost(), cfg.getPort(), cfg.getDatabase(), cfg.getSchema(), cfg.getCacheTable());

        PostgresqlConnectionConfiguration.Builder builder = PostgresqlConnectionConfiguration.builder()
                .host(cfg.getHost())
                .port(cfg.getPort());
        if (cfg.getDatabase() != null) {
            builder.database(cfg.getDatabase());
        }
        if (cfg.getUsername() != null) {
            builder.username(cfg.getUsername());
        }
        if (cfg.getPassword() != null) {
            builder.password(cfg.getPassword());
        }
        if (cfg.getSchema() != null) {
            builder.schema(cfg.getSchema());
        }
        if (cfg.getProperties() != null && !cfg.getProperties().isEmpty()) {
            Map<String, String> options = new HashMap<>();
            for (Map.Entry<String, Object> entry : cfg.getProperties().entrySet()) {
                if (entry.getValue() != null) {
                    options.put(entry.getKey(), entry.getValue().toString());
                }
            }
            if (!options.isEmpty()) {
                builder.options(options);
            }
        }
        PostgresqlConnectionFactory delegate = new PostgresqlConnectionFactory(builder.build());

        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(delegate)
                .maxSize(Math.max(1, cfg.getMaxPoolSize()))
                .initialSize(Math.max(1, cfg.getMinPoolSize()))
                .maxIdleTime(Duration.ofMinutes(5))
                .build();
        ConnectionPool pool = new ConnectionPool(poolConfig);

        if (cfg.isAutoCreateSchema()) {
            initialiseSchema(pool, cfg);
        }

        log.info("PostgreSQL cache ConnectionFactory created successfully");
        return pool;
    }

    /**
     * Provisions the cache table and a supporting expiry index. Statements are
     * idempotent so repeated startups are safe.
     */
    private void initialiseSchema(ConnectionFactory factory, CacheProperties.PostgresConfig cfg) {
        String schema = cfg.getSchema() != null ? cfg.getSchema() : "public";
        String table = cfg.getCacheTable() != null ? cfg.getCacheTable() : "firefly_cache_entries";
        String qualified = "\"" + schema + "\".\"" + table + "\"";

        String createTable = "CREATE TABLE IF NOT EXISTS " + qualified + " ("
                + "cache_name VARCHAR(255) NOT NULL, "
                + "cache_key VARCHAR(1024) NOT NULL, "
                + "value BYTEA NOT NULL, "
                + "expires_at TIMESTAMPTZ, "
                + "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "PRIMARY KEY (cache_name, cache_key))";

        String createExpiresIndex = "CREATE INDEX IF NOT EXISTS " + table + "_expires_idx "
                + "ON " + qualified + " (expires_at) WHERE expires_at IS NOT NULL";

        try {
            Mono.usingWhen(
                            Mono.from(factory.create()),
                            connection -> executeAll(connection, createTable, createExpiresIndex),
                            Connection::close)
                    .block(Duration.ofSeconds(30));
            log.info("PostgreSQL cache schema ensured: {}.{}", schema, table);
        } catch (Exception e) {
            log.error("Failed to initialise PostgreSQL cache schema {}.{}: {}", schema, table, e.getMessage());
            log.error("Caching will fail until the schema is created manually");
        }
    }

    private Mono<Void> executeAll(Connection connection, String... statements) {
        Mono<Void> chain = Mono.empty();
        for (String sql : statements) {
            String stmt = sql;
            chain = chain.then(Mono.from(connection.createStatement(stmt).execute())
                    .flatMap(result -> Mono.from(result.getRowsUpdated()))
                    .then());
        }
        return chain;
    }
}
