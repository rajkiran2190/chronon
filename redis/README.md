# Redis KVStore Implementation

This module provides a Redis Cluster-based KVStore implementation for Chronon.

## Features

- **Redis Cluster Support**: Uses JedisCluster for Redis cluster connectivity
- **Hash Tags**: Uses hash tags `{base64_key}` to ensure all data for an entity lands on the same cluster node
- **Time-Series Data**: Supports time-series data using Redis sorted sets with Last-Write-Wins semantics
- **TTL Management**: Default 5-day TTL matching BigTable implementation
- **Connection Pooling**: Configurable connection pool settings
- **Comprehensive Tests**: 29+ integration tests using Testcontainers with real Redis cluster

## Configuration

Configuration is loaded from environment variables or user conf map:

- `REDIS_CLUSTER_NODES`: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379,node3:6379")
- `REDIS_PASSWORD`: Redis password (optional)
- `REDIS_MAX_CONNECTIONS`: Maximum pool connections (default: 50)
- `REDIS_MIN_IDLE_CONNECTIONS`: Minimum idle connections (default: 5)
- `REDIS_MAX_IDLE_CONNECTIONS`: Maximum idle connections (default: 10)
- `REDIS_CONNECTION_TIMEOUT_MS`: Connection timeout in milliseconds (default: 5000)
- `REDIS_SO_TIMEOUT_MS`: Socket timeout in milliseconds (default: 2000)
- `REDIS_MAX_REDIRECTIONS`: Maximum cluster redirections (default: 5)
- `REDIS_KEY_PREFIX`: Key prefix for namespace isolation (default: "chronon")

## Key Structure

- **Batch IRs**: `chronon:{dataset}:{base64_key}` (stored as strings with 8-byte timestamp prefix)
- **Time-series**: `chronon:{dataset}:{base64_key}:{dayTs}` (stored as sorted sets)
- **Tiled data**: `chronon:{dataset}:{base64_key}:{dayTs}:{tileSize}` (stored as sorted sets)

Hash tags `{base64_key}` ensure all data for an entity lands on the same cluster node, enabling efficient pipelining for multi-day queries and co-locating batch IR with streaming tiles.

## Running Tests

```bash
# Run all Redis tests
./mill redis.test

# Or use the helper script
./redis/run-tests.sh
```

Tests use Testcontainers with `grokzen/redis-cluster:7.0.10` which provides a 6-node cluster (3 masters, 3 replicas) in a single container.

## Dependencies

- `redis.clients:jedis:5.1.0` - Redis client
- `org.apache.commons:commons-pool2:2.12.0` - Connection pooling
- `org.testcontainers:testcontainers:1.19.3` - For integration tests

