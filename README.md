# Firefly Framework - Cache - Postgres

[![CI](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/fireflyframework/fireflyframework-cache-postgres/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)

> PostgreSQL R2DBC implementation of the Firefly cache provider with TTL-based expiry.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Firefly Framework Cache Postgres implements the `CacheAdapter` port from `fireflyframework-cache` using PostgreSQL as a persistent, distributed cache backend over reactive R2DBC. Cache entries live in a single relational table keyed by `(cache_name, cache_key)`, with values serialized to JSON and stored in a `BYTEA` column. Time-to-live is enforced through an `expires_at` timestamp column that is checked lazily on every read.

The module contributes a `PostgresProvider` (discovered via `ServiceLoader`) and a `PostgresCacheAutoConfiguration` that builds a pooled `ConnectionFactory` and optionally provisions the cache schema on startup. Once on the classpath with `firefly.cache.postgres.enabled=true`, the cache core auto-selects PostgreSQL when `firefly.cache.default-cache-type=POSTGRES` (or via `AUTO`).

All operations are fully non-blocking, acquiring and releasing connections per operation from the underlying `io.r2dbc.pool.ConnectionPool`.

## Features

- Full `CacheAdapter` implementation backed by PostgreSQL R2DBC
- Reactive, non-blocking operations via `Mono.usingWhen` connection lifecycle
- TTL support with lazy expiry on read (`expires_at IS NULL OR expires_at > NOW()`)
- JSON serialization to a `BYTEA` column via the framework `CacheSerializer`
- Atomic `putIfAbsent` using `INSERT ... ON CONFLICT DO NOTHING`
- Native prefix eviction via `DELETE ... WHERE cache_key LIKE prefix || '%'`
- Pooled connections with configurable pool sizing
- Optional automatic schema creation (table + expiry index)
- Connection health checks via `Connection.validate(ValidationDepth.REMOTE)`
- In-process statistics (hits, misses, puts, evictions)
- `ServiceLoader` SPI registration and Spring Boot auto-configuration

## Requirements

- Java 21+
- Spring Boot 3.x
- Maven 3.9+
- PostgreSQL 12+

## Installation

```xml
<dependency>
    <groupId>org.fireflyframework</groupId>
    <artifactId>fireflyframework-cache-postgres</artifactId>
    <version>26.05.07</version>
</dependency>
```

## Quick Start

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

## Configuration

```yaml
firefly:
  cache:
    default-cache-type: POSTGRES
    postgres:
      enabled: true
      host: localhost
      port: 5432
      database: appdb
      username: app
      password: secret
      schema: public
      cache-table: firefly_cache_entries
      auto-create-schema: true
      max-pool-size: 10
      default-ttl: PT30M
```

| Property | Description | Default |
|---|---|---|
| `firefly.cache.postgres.enabled` | Enables the PostgreSQL cache provider | `false` |
| `firefly.cache.postgres.host` | PostgreSQL host | `localhost` |
| `firefly.cache.postgres.port` | PostgreSQL port | `5432` |
| `firefly.cache.postgres.database` | Database name | _(none)_ |
| `firefly.cache.postgres.username` | Database username | _(none)_ |
| `firefly.cache.postgres.password` | Database password | _(none)_ |
| `firefly.cache.postgres.schema` | Schema holding the cache table | `public` |
| `firefly.cache.postgres.cache-table` | Cache table name | `firefly_cache_entries` |
| `firefly.cache.postgres.auto-create-schema` | Create table + index on startup | `true` |
| `firefly.cache.postgres.max-pool-size` | Maximum pool connections | `10` |
| `firefly.cache.postgres.default-ttl` | Default entry TTL | `PT30M` |

The provider stores entries in:

```sql
CREATE TABLE IF NOT EXISTS "public"."firefly_cache_entries" (
  cache_name VARCHAR(255) NOT NULL,
  cache_key  VARCHAR(1024) NOT NULL,
  value      BYTEA NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (cache_name, cache_key)
);
CREATE INDEX IF NOT EXISTS firefly_cache_entries_expires_idx
  ON "public"."firefly_cache_entries" (expires_at) WHERE expires_at IS NOT NULL;
```

## Documentation

No additional documentation available for this project.

## Contributing

Contributions are welcome. Please read the [CONTRIBUTING.md](CONTRIBUTING.md) guide for details on our code of conduct, development process, and how to submit pull requests.

## License

Copyright 2024-2026 Firefly Software Foundation.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
