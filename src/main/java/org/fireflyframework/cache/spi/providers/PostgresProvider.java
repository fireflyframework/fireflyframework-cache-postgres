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

package org.fireflyframework.cache.spi.providers;

import org.fireflyframework.cache.core.CacheAdapter;
import org.fireflyframework.cache.core.CacheType;
import org.fireflyframework.cache.factory.PostgresCacheHelper;
import org.fireflyframework.cache.spi.CacheProviderFactory;

import java.time.Duration;

/**
 * {@link CacheProviderFactory} implementation that contributes the PostgreSQL
 * R2DBC cache provider. Discovered via {@code ServiceLoader}.
 */
public class PostgresProvider implements CacheProviderFactory {

    @Override
    public CacheType getType() {
        return CacheType.POSTGRES;
    }

    @Override
    public int priority() {
        return 15;
    }

    @Override
    public boolean isAvailable(ProviderContext ctx) {
        try {
            Class.forName("io.r2dbc.spi.ConnectionFactory");
            return ctx.r2dbcConnectionFactory != null && ctx.properties.getPostgres().isEnabled();
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public CacheAdapter create(String cacheName, String keyPrefix, Duration defaultTtl, ProviderContext ctx) {
        return PostgresCacheHelper.createPostgresCacheAdapter(
                cacheName,
                keyPrefix,
                defaultTtl,
                ctx.r2dbcConnectionFactory,
                ctx.properties,
                ctx.objectMapper);
    }
}
