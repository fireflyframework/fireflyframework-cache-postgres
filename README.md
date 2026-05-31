# Firefly Framework - Cache - Postgres

[![CI](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> Durable PostgreSQL/R2DBC cache provider adapter for the Firefly Framework cache abstraction — a fully reactive, SQL-backed `CacheAdapter` with TTL expiry, atomic put-if-absent, and prefix eviction.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

`fireflyframework-cache-postgres` is one of the pluggable cache **provider adapters** for the Firefly
Framework cache abstraction. It implements the `CacheAdapter` port defined in
[`fireflyframework-cache`](https://github.com/fireflyframework/fireflyframework-cache) (the cache core)
using **PostgreSQL** as a persistent, distributed cache backend over fully reactive **R2DBC**.

The cache core ships **Caffeine** as the in-process default. Every other backend — Redis, Hazelcast,
JCache and PostgreSQL — lives in its own adapter module, so an application only pulls in the
dependency it actually uses. This module contributes a `PostgresProvider` (discovered through the
`CacheProviderFactory` `ServiceLoader` SPI) and a `PostgresCacheAutoConfiguration`. Once on the
classpath with `firefly.cache.postgres.enabled=true`, the cache core makes the `POSTGRES` cache type
available to `FireflyCacheManager` and selects it when `firefly.cache.default-cache-type=POSTGRES`
(or via `AUTO`).

Use this adapter when you want a **durable, SQL-backed cache** rather than an in-memory or Redis
cache — for example, when entries must survive process restarts, when cache contents should be
queryable and auditable through ordinary SQL, or when you would rather reuse your existing PostgreSQL
infrastructure than operate a separate cache cluster. Cache entries live in a single relational table
keyed by `(cache_name, cache_key)`, with values serialized to JSON and stored in a `BYTEA` column;
time-to-live is enforced through an `expires_at` timestamp checked lazily on every read. Because the
adapter is built on R2DBC and Project Reactor, every operation is non-blocking and integrates cleanly
with Spring WebFlux and the rest of the Firefly Framework.

### Where it sits in the framework

| Module | Role |
| --- | --- |
| `fireflyframework-cache` | Cache abstraction + SPI + Caffeine default (`CacheAdapter`, `CacheType`, `CacheProviderFactory`, `FireflyCacheManager`) |
| `fireflyframework-cache-redis` | Redis provider adapter |
| `fireflyframework-cache-hazelcast` | Hazelcast provider adapter |
| `fireflyframework-cache-jcache` | JCache (JSR-107) provider adapter |
| **`fireflyframework-cache-postgres`** | **PostgreSQL / R2DBC provider adapter (this module)** |

## Features

- Full `CacheAdapter` implementation (`PostgresCacheAdapter`) backed by PostgreSQL R2DBC.
- Reactive, non-blocking operations with per-operation connection lifecycle via `Mono.usingWhen`.
- TTL support with lazy expiry on read (`expires_at IS NULL OR expires_at > NOW()`) and write-time `expires_at = NOW() + ttl`.
- JSON serialization to a `BYTEA` column through the framework `CacheSerializer` (`JsonCacheSerializer`).
- Atomic `putIfAbsent` using `INSERT ... ON CONFLICT DO NOTHING`; upserts via `ON CONFLICT DO UPDATE`.
- Native prefix eviction (`evictByPrefix`) via `DELETE ... WHERE cache_key LIKE prefix || '%'`.
- Per-cache key namespacing (`<keyPrefix>:<cacheName>:<key>`) so one table hosts many named caches.
- Pooled connections via `r2dbc-pool` with configurable min/max pool sizing.
- Optional automatic schema creation (cache table + partial expiry index) on startup.
- Connection health checks via `Connection.validate(ValidationDepth.REMOTE)`, surfaced as `CacheHealth`.
- In-process statistics (requests, hits, misses, puts, evictions, entry count) as `CacheStats`.
- `ServiceLoader` SPI registration (`PostgresProvider`, `CacheType.POSTGRES`, priority 15) plus Spring Boot auto-configuration.

## Requirements

- Java 21+ (Java 25 recommended)
- Spring Boot 3.x
- Maven 3.9+
- PostgreSQL 12+ reachable over R2DBC
- `fireflyframework-cache` on the classpath (pulled in transitively as a dependency)

## Installation

Add the adapter to your application. The version is managed by the Firefly Framework parent POM / BOM,
so you normally omit `<version>`:

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-cache-postgres</artifactId>
</dependency>
```

This brings in `fireflyframework-cache` (the cache abstraction), the `r2dbc-postgresql` driver,
`r2dbc-pool` and the R2DBC SPI transitively.

If you are not inheriting the Firefly parent, import the BOM in your `dependencyManagement` and let it
manage the version:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>org.fireflyframework</groupId>
            <artifactId>fireflyframework-bom</artifactId>
            <version>${firefly.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## Quick Start

1. **Add the dependencies** — the cache core and this adapter:

```xml
<dependencies>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-cache</artifactId>
    </dependency>
    <dependency>
        <groupId>org.fireflyframework</groupId>
        <artifactId>fireflyframework-cache-postgres</artifactId>
    </dependency>
</dependencies>
```

2. **Enable the provider and point it at your database** in `application.yml`:

```yaml
firefly:
  cache:
    default-cache-type: POSTGRES   # select this adapter (or AUTO to let the core pick)
    postgres:
      enabled: true
      host: localhost
      port: 5432
      database: appdb
      username: app
      password: secret
```

3. **Use the unified cache API** — obtain a `CacheAdapter` from the core `FireflyCacheManager`; it is
   now backed by PostgreSQL with no other code changes:

```java
import org.fireflyframework.cache.core.CacheAdapter;
import org.fireflyframework.cache.manager.FireflyCacheManager;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductService {

    private final CacheAdapter cache;

    public ProductService(FireflyCacheManager cacheManager) {
        this.cache = cacheManager.getCache("products"); // CacheType.POSTGRES when selected
    }

    public Mono<Product> findById(String id) {
        return cache.<String, Product>get(id, Product.class)
            .flatMap(opt -> opt.map(Mono::just).orElseGet(() ->
                loadFromDatabase(id).flatMap(p -> cache.put(id, p).thenReturn(p))));
    }
}
```

Because every adapter implements the same `CacheAdapter` SPI, switching between Caffeine, Redis,
Hazelcast, JCache and PostgreSQL is a configuration change (`firefly.cache.default-cache-type`) plus
the matching dependency — application code stays the same.

## Configuration

All adapter-specific properties live under the `firefly.cache.postgres` prefix and are bound by the
core `CacheProperties.PostgresConfig`. The active provider is chosen by the core-owned
`firefly.cache.default-cache-type` key.

```yaml
firefly:
  cache:
    default-cache-type: POSTGRES    # core: choose the active provider (default: CAFFEINE)
    postgres:
      enabled: true                 # default: false
      host: localhost               # default: localhost
      port: 5432                    # default: 5432
      database: appdb               # default: none
      username: app                 # default: none
      password: secret              # default: none
      schema: public                # default: public
      cache-table: firefly_cache_entries  # default: firefly_cache_entries
      cache-name: default           # default: default
      key-prefix: ""                # default: empty
      default-ttl: PT30M            # default: 30 minutes
      auto-create-schema: true      # default: true
      max-pool-size: 10             # default: 10
      min-pool-size: 1              # default: 1
      properties: {}                # extra R2DBC driver options
```

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `firefly.cache.default-cache-type` | `CacheType` | `CAFFEINE` | Core property selecting the active provider. Set to `POSTGRES` (or `AUTO`) to use this adapter. |
| `firefly.cache.postgres.enabled` | `boolean` | `false` | Enables the PostgreSQL cache provider and its auto-configuration. |
| `firefly.cache.postgres.host` | `String` | `localhost` | PostgreSQL host. |
| `firefly.cache.postgres.port` | `int` | `5432` | PostgreSQL port. |
| `firefly.cache.postgres.database` | `String` | — | Database name. |
| `firefly.cache.postgres.username` | `String` | — | Database username. |
| `firefly.cache.postgres.password` | `String` | — | Database password. |
| `firefly.cache.postgres.schema` | `String` | `public` | Schema that holds the cache table. |
| `firefly.cache.postgres.cache-table` | `String` | `firefly_cache_entries` | Cache table name. |
| `firefly.cache.postgres.cache-name` | `String` | `default` | Default logical cache name. |
| `firefly.cache.postgres.key-prefix` | `String` | `""` | Prefix applied to namespaced keys. |
| `firefly.cache.postgres.default-ttl` | `Duration` | `PT30M` | Default entry TTL; zero/negative means no expiration. |
| `firefly.cache.postgres.auto-create-schema` | `boolean` | `true` | Create the cache table and expiry index on startup. |
| `firefly.cache.postgres.max-pool-size` | `int` | `10` | Maximum R2DBC pool connections. |
| `firefly.cache.postgres.min-pool-size` | `int` | `1` | Initial/minimum R2DBC pool connections. |
| `firefly.cache.postgres.properties` | `Map` | `{}` | Extra options passed to the PostgreSQL R2DBC driver. |

> Connection settings come **only** from `firefly.cache.postgres.*`, never from `spring.r2dbc.*` —
> the cache uses a dedicated, pooled `ConnectionFactory` bean
> (`fireflyCachePostgresConnectionFactory`) independent of your application datasource.

### Storage schema

When `auto-create-schema` is `true`, the adapter provisions (idempotently) the following table and a
partial index supporting expiry scans:

```sql
CREATE TABLE IF NOT EXISTS "public"."firefly_cache_entries" (
  cache_name VARCHAR(255)  NOT NULL,
  cache_key  VARCHAR(1024) NOT NULL,
  value      BYTEA         NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (cache_name, cache_key)
);
CREATE INDEX IF NOT EXISTS firefly_cache_entries_expires_idx
  ON "public"."firefly_cache_entries" (expires_at) WHERE expires_at IS NOT NULL;
```

If you disable `auto-create-schema`, create this table and index manually before first use.

## How It Works

- **`PostgresCacheAutoConfiguration`** (`org.fireflyframework.cache.config`) — Spring Boot
  `@AutoConfiguration` registered in
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Activates when
  the R2DBC SPI is on the classpath and `firefly.cache.postgres.enabled=true`. Builds the pooled
  `fireflyCachePostgresConnectionFactory` bean and, when enabled, provisions the schema.
- **`PostgresProvider`** (`org.fireflyframework.cache.spi.providers`) — the `CacheProviderFactory` SPI
  implementation, registered for `ServiceLoader` in
  `META-INF/services/org.fireflyframework.cache.spi.CacheProviderFactory`. Reports `CacheType.POSTGRES`
  with priority 15 and is available when the connection factory is present and the provider is enabled.
- **`PostgresCacheHelper`** (`org.fireflyframework.cache.factory`) — the factory helper the cache core
  references reflectively to build adapters; its fully-qualified name is load-bearing and must not be
  renamed or moved.
- **`PostgresCacheAdapter`** (`org.fireflyframework.cache.adapter.postgres`) — the `CacheAdapter`
  implementation. Stores entries in the relational table via the R2DBC `ConnectionFactory`, serializes
  values with `JsonCacheSerializer`, namespaces keys per cache, enforces TTL, and reports
  `CacheStats`/`CacheHealth`.
- **`PostgresCacheConfig`** (`org.fireflyframework.cache.adapter.postgres`) — immutable per-adapter
  config (cache name, key prefix, default TTL, schema, table).

## Documentation

- Firefly Framework documentation hub and module catalog: <https://github.com/fireflyframework>
- Cache abstraction & SPI: [`fireflyframework-cache`](https://github.com/fireflyframework/fireflyframework-cache)
- Sibling adapters:
  [Redis](https://github.com/fireflyframework/fireflyframework-cache-redis) ·
  [Hazelcast](https://github.com/fireflyframework/fireflyframework-cache-hazelcast) ·
  [JCache](https://github.com/fireflyframework/fireflyframework-cache-jcache)

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
