# Firefly Framework - Cache PostgreSQL Adapter

[![CI](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> Reactive, durable PostgreSQL/R2DBC cache provider adapter for the Firefly Framework cache abstraction — a SQL-backed, non-blocking `FireflyCache` implementation with TTL expiration and background cleanup.

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
Framework cache abstraction. It implements the cache SPI defined in
[`fireflyframework-cache-core`](https://github.com/fireflyframework/fireflyframework-cache-core)
(`FireflyCache`, `CacheProvider`, `CacheType`) on top of **PostgreSQL** using
**R2DBC** for fully reactive, non-blocking database access.

The cache core ships **Caffeine** as the in-process default. Every other backend — Redis, Hazelcast,
JCache and PostgreSQL — lives in its own adapter module so applications only pull in the dependency
they actually use. This module is the PostgreSQL adapter: dropping it on the classpath and selecting
`firefly.cache.provider=postgres` routes the unified cache API to a relational table instead of an
in-memory store.

Use this adapter when you want a **durable, SQL-backed cache** rather than an in-memory or Redis
cache — for example, when entries must survive process restarts, when you want cache contents to be
queryable/auditable through ordinary SQL, or when you would rather reuse your existing PostgreSQL
infrastructure than operate a separate cache cluster. Because it is built on R2DBC and Project
Reactor, it integrates cleanly with reactive Spring WebFlux stacks and the rest of the Firefly
Framework without introducing blocking calls.

### Where it sits in the framework

| Module | Role |
| --- | --- |
| `fireflyframework-cache-core` | Cache SPI + Caffeine default (`FireflyCache`, `CacheProvider`, `CacheManager`, `CacheType`) |
| `fireflyframework-cache-redis` | Redis provider adapter |
| `fireflyframework-cache-hazelcast` | Hazelcast provider adapter |
| `fireflyframework-cache-jcache` | JCache (JSR-107) provider adapter |
| **`fireflyframework-cache-postgres`** | **PostgreSQL / R2DBC provider adapter (this module)** |

## Features

- **Reactive end to end** — implements `FireflyCache` over R2DBC; no blocking JDBC calls, suitable for Spring WebFlux and reactive Firefly services.
- **Durable, SQL-backed storage** — cache entries persist in a relational table (`firefly_cache` by default) so they survive restarts and remain queryable with plain SQL.
- **Reports as `CacheType.POSTGRES`** — slots into the cache abstraction alongside Caffeine, Redis, Hazelcast and JCache via the same `CacheProvider` SPI.
- **TTL-based expiration** — per-entry time-to-live with a configurable default; zero/negative means no expiration.
- **Background cleanup** — a periodic purge job removes expired rows on a configurable interval, keeping the table lean.
- **Connection pooling** — uses `r2dbc-pool` for efficient, bounded reactive connection management.
- **Jackson serialization** — values are serialized via `jackson-databind` for compact, portable storage.
- **Spring Boot auto-configuration** — `PostgresCacheAutoConfiguration` wires everything automatically; gated by `@ConditionalOnClass(CacheProvider.class)` and the `firefly.cache.postgres.enabled` property.
- **Type-safe configuration** — all settings exposed through `PostgresCacheProperties` under the `firefly.cache.postgres` prefix.

## Requirements

- Java 21+ (Java 25 recommended)
- Spring Boot 3.x
- Maven 3.9+
- A reachable PostgreSQL database (any supported version) with R2DBC connectivity
- `fireflyframework-cache-core` on the classpath (pulled in transitively as a dependency)

## Installation

Add the adapter to your application. The version is managed by the Firefly Framework
parent POM / BOM, so you normally omit `<version>`:

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-cache-postgres</artifactId>
</dependency>
```

This brings in `fireflyframework-cache-core` (the cache SPI), the `r2dbc-postgresql` driver,
`r2dbc-pool` and `jackson-databind` transitively.

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

The adapter is auto-configured. To activate the PostgreSQL backend:

1. **Add the dependency** (see [Installation](#installation)).
2. **Select the provider** and point it at your database in `application.yml`:

```yaml
firefly:
  cache:
    provider: postgres          # route the unified cache abstraction to this adapter
    postgres:
      url: r2dbc:postgresql://localhost:5432/mydb
      username: firefly
      password: ${DB_PASSWORD}
      table-name: firefly_cache
      default-ttl: 10m
      cleanup-interval: 5m
```

3. **Use the unified cache API** — inject the `FireflyCache` (or higher-level cache facade) provided by
   `fireflyframework-cache-core`; it is now backed by PostgreSQL with no other code changes:

```java
import org.fireflyframework.cache.FireflyCache;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ProductService {

    private final FireflyCache cache;

    public ProductService(FireflyCache cache) {
        this.cache = cache; // CacheType.POSTGRES when this adapter is selected
    }

    public Mono<Product> findById(String id) {
        return cache.get(id, Product.class)
            .switchIfEmpty(loadFromDatabase(id)
                .flatMap(product -> cache.put(id, product).thenReturn(product)));
    }
}
```

Because every adapter implements the same `FireflyCache` SPI, switching between Caffeine, Redis,
Hazelcast, JCache and PostgreSQL is a configuration change (`firefly.cache.provider`) plus the matching
dependency — application code stays the same.

## Configuration

All adapter-specific properties live under the `firefly.cache.postgres` prefix and are bound by
`PostgresCacheProperties`. The provider is selected by the core-owned `firefly.cache.provider` key.

```yaml
firefly:
  cache:
    provider: postgres            # core: choose the active provider (default: caffeine)
    postgres:
      enabled: true               # default: true
      url:                        # R2DBC URL, e.g. r2dbc:postgresql://localhost:5432/mydb
      username:                   # database username
      password:                   # database password
      table-name: firefly_cache   # default: firefly_cache
      default-ttl: 0s             # default: 0 (no expiration)
      cleanup-interval: 5m        # default: 5 minutes
```

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `firefly.cache.provider` | `CacheType` | `caffeine` | Core property that selects the active provider. Set to `postgres` to use this adapter. |
| `firefly.cache.postgres.enabled` | `boolean` | `true` | Enables/disables the PostgreSQL cache auto-configuration. |
| `firefly.cache.postgres.url` | `String` | — | R2DBC connection URL (e.g. `r2dbc:postgresql://host:5432/db`). |
| `firefly.cache.postgres.username` | `String` | — | Database username. |
| `firefly.cache.postgres.password` | `String` | — | Database password. |
| `firefly.cache.postgres.table-name` | `String` | `firefly_cache` | Table that stores cache entries. |
| `firefly.cache.postgres.default-ttl` | `Duration` | `0s` (none) | Default time-to-live for entries; zero or negative means no expiration. |
| `firefly.cache.postgres.cleanup-interval` | `Duration` | `5m` | How often the background job purges expired entries. |

## How It Works

- **`PostgresCacheAutoConfiguration`** — Spring Boot `@AutoConfiguration` registered in
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Activates when
  `CacheProvider` is on the classpath and `firefly.cache.postgres.enabled` is `true` (the default),
  and enables `PostgresCacheProperties`.
- **`PostgresCacheConfiguration`** — wires the resolved `PostgresCacheProperties` into the provider.
- **`PostgresFireflyCache`** — the `FireflyCache` implementation. Stores entries in a relational table
  via an R2DBC `ConnectionFactory`, serializes values with Jackson, reports `CacheType.POSTGRES`, and
  supports TTL-based expiration with periodic cleanup of stale rows.
- **`PostgresCacheManager`** — manages the lifecycle of PostgreSQL-backed cache instances.

## Documentation

- Firefly Framework documentation hub and module catalog: <https://github.com/fireflyframework>
- Cache abstraction SPI: [`fireflyframework-cache-core`](https://github.com/fireflyframework/fireflyframework-cache-core)
- Sibling adapters:
  [Redis](https://github.com/fireflyframework/fireflyframework-cache-redis) ·
  [Hazelcast](https://github.com/fireflyframework/fireflyframework-cache-hazelcast) ·
  [JCache](https://github.com/fireflyframework/fireflyframework-cache-jcache)

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
