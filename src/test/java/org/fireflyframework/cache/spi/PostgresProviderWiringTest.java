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

package org.fireflyframework.cache.spi;

import org.fireflyframework.cache.core.CacheType;
import org.fireflyframework.cache.spi.providers.PostgresProvider;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the PostgreSQL cache provider is discoverable via {@link ServiceLoader}
 * and advertises the correct {@link CacheType} and priority.
 */
class PostgresProviderWiringTest {

    @Test
    void serviceLoaderDiscoversPostgresProvider() {
        ServiceLoader<CacheProviderFactory> loader = ServiceLoader.load(CacheProviderFactory.class);

        boolean found = false;
        for (CacheProviderFactory factory : loader) {
            if (factory.getType() == CacheType.POSTGRES) {
                found = true;
                assertThat(factory).isInstanceOf(PostgresProvider.class);
                assertThat(factory.priority()).isEqualTo(15);
            }
        }

        assertThat(found)
                .as("ServiceLoader should discover a CacheProviderFactory with type POSTGRES")
                .isTrue();
    }

    @Test
    void postgresProviderReportsExpectedType() {
        PostgresProvider provider = new PostgresProvider();
        assertThat(provider.getType()).isEqualTo(CacheType.POSTGRES);
        assertThat(provider.priority()).isEqualTo(15);
    }
}
