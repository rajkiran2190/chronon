package ai.chronon.integrations.redis

/**
 * Constants for Redis KVStore configuration and operation.
 * These can be overridden via environment variables or userConf in RedisApiImpl.
 */
object RedisKVStoreConstants {
  // Key structure
  // Note: DefaultKeyPrefix can be set to empty string ("") for dedicated Redis deployments
  // to save memory (~8 bytes per key). Default "chronon" provides namespace isolation.
  val DefaultKeyPrefix = "chronon"
  val KeySeparator = ":"

  // TTL configuration (matching BigTable's 5-day retention)
  val DataTTLSeconds = 5 * 24 * 60 * 60 // 5 days

  // Connection pool defaults
  val DefaultMaxConnections = 50
  val DefaultMinIdleConnections = 5
  val DefaultMaxIdleConnections = 10
  val DefaultConnectionTimeoutMs = 5000
  val DefaultPort = 6379

  // Pagination defaults
  val DefaultListLimit = 100

  // Redis Cluster configuration
  val DefaultMaxRedirections = 5
  val DefaultSoTimeoutMs = 2000

  // Environment variable keys for Redis Cluster
  val EnvRedisClusterNodes = "REDIS_CLUSTER_NODES" // Comma-separated: "node1:6379,node2:6379,node3:6379"
  val EnvRedisPassword = "REDIS_PASSWORD"
  val EnvRedisKeyPrefix = "REDIS_KEY_PREFIX"
  val EnvRedisMaxConnections = "REDIS_MAX_CONNECTIONS"
  val EnvRedisMinIdleConnections = "REDIS_MIN_IDLE_CONNECTIONS"
  val EnvRedisMaxIdleConnections = "REDIS_MAX_IDLE_CONNECTIONS"
  val EnvRedisConnectionTimeoutMs = "REDIS_CONNECTION_TIMEOUT_MS"
  val EnvRedisSoTimeoutMs = "REDIS_SO_TIMEOUT_MS"
  val EnvRedisMaxRedirections = "REDIS_MAX_REDIRECTIONS"

  // Prop keys for create()
  val PropTTLSeconds = "ttl-seconds"
  val PropMaxConnections = "max-connections"
  val PropKeyPrefix = "key-prefix"
}

