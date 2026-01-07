package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.jdk.CollectionConverters._

/** Factory for creating Redis KVStore instances.
  *
  * This is cloud-agnostic and can be used by any Api implementation (GcpApiImpl, AwsApiImpl, etc.)
  * or directly in tests and custom applications.
  *
  * Configuration is loaded from environment variables or provided conf map:
  *  - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379,node3:6379") [required]
  *  - REDIS_PASSWORD: Redis password (optional)
  *  - REDIS_MAX_CONNECTIONS: Maximum pool connections (default: 50)
  *  - REDIS_MIN_IDLE_CONNECTIONS: Minimum idle connections (default: 5)
  *  - REDIS_MAX_IDLE_CONNECTIONS: Maximum idle connections (default: 10)
  *  - REDIS_CONNECTION_TIMEOUT_MS: Connection timeout in milliseconds (default: 5000)
  *  - REDIS_SO_TIMEOUT_MS: Socket timeout in milliseconds (default: 2000)
  *  - REDIS_MAX_REDIRECTIONS: Maximum cluster redirections (default: 5)
  *
  * Example usage:
  * {{{
  *  // In GcpApiImpl or AwsApiImpl
  *  override def genKvStore: KVStore = {
  *    if (shouldUseRedis) RedisKVStoreFactory.create(conf)
  *    else createDefaultKVStore()
  *  }
  *
  *  // In tests
  *  val testConf = Map("REDIS_CLUSTER_NODES" -> "localhost:7000")
  *  val kvStore = RedisKVStoreFactory.create(testConf)
  * }}}
  */
object RedisKVStoreFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Create a Redis KVStore instance from configuration.
    *
    * @param conf Configuration map (environment variables take precedence)
    * @return Initialized RedisKVStoreImpl ready for use
    * @throws IllegalArgumentException if REDIS_CLUSTER_NODES is not set
    */
  def create(conf: Map[String, String]): RedisKVStoreImpl = {
    // Parse cluster nodes from environment or config
    val nodesStr = getOrElseThrow(EnvRedisClusterNodes, conf)
    val password = getOptional(EnvRedisPassword, conf)

    val maxConnections = getOptional(EnvRedisMaxConnections, conf).map(_.toInt).getOrElse(DefaultMaxConnections)
    val minIdleConnections =
      getOptional(EnvRedisMinIdleConnections, conf).map(_.toInt).getOrElse(DefaultMinIdleConnections)
    val maxIdleConnections =
      getOptional(EnvRedisMaxIdleConnections, conf).map(_.toInt).getOrElse(DefaultMaxIdleConnections)
    val connectionTimeoutMs =
      getOptional(EnvRedisConnectionTimeoutMs, conf).map(_.toInt).getOrElse(DefaultConnectionTimeoutMs)
    val soTimeoutMs = getOptional(EnvRedisSoTimeoutMs, conf).map(_.toInt).getOrElse(DefaultSoTimeoutMs)
    val maxRedirections = getOptional(EnvRedisMaxRedirections, conf).map(_.toInt).getOrElse(DefaultMaxRedirections)

    // Parse cluster nodes: "node1:6379,node2:6379,node3:6379"
    val clusterNodes = nodesStr
      .split(",")
      .map { node =>
        val parts = node.trim.split(":")
        if (parts.length == 2) {
          new HostAndPort(parts(0), parts(1).toInt)
        } else {
          new HostAndPort(parts(0), DefaultPort)
        }
      }
      .toSet

    logger.info(
      s"Creating Redis Cluster KVStore with nodes: ${clusterNodes.mkString(", ")}." +
        s"Params: maxConnections=$maxConnections, minIdle=$minIdleConnections, " +
        s"maxIdle=$maxIdleConnections, connectionTimeout=$connectionTimeoutMs, " +
        s"soTimeout=$soTimeoutMs, maxRedirections=$maxRedirections"
    )

    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(maxConnections)
    poolConfig.setMaxIdle(maxIdleConnections)
    poolConfig.setMinIdle(minIdleConnections)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)

    val jedisCluster = password match {
      case Some(pwd) =>
        new JedisCluster(
          clusterNodes.asJava,
          connectionTimeoutMs,
          soTimeoutMs,
          maxRedirections,
          pwd,
          poolConfig.asInstanceOf[org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]]
        )

      case None =>
        new JedisCluster(
          clusterNodes.asJava,
          connectionTimeoutMs,
          soTimeoutMs,
          maxRedirections,
          poolConfig.asInstanceOf[org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]]
        )
    }

    val kvStore = new RedisKVStoreImpl(jedisCluster, conf)
    kvStore.init()
    kvStore
  }

  private def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env.get(key).orElse(conf.get(key))

  private def getOrElseThrow(key: String, conf: Map[String, String]): String =
    getOptional(key, conf).getOrElse(
      throw new IllegalArgumentException(s"$key environment variable not set")
    )
}
