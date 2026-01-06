package ai.chronon.integrations.redis

import ai.chronon.online._
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, SerDe}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{HostAndPort, JedisCluster}
import redis.clients.jedis.JedisPoolConfig

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

/**
 * Redis Cluster-based Api implementation.
 *
 * Configuration is loaded from environment variables or user conf map:
 * - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379,node3:6379")
 * - REDIS_PASSWORD: Redis password (optional)
 * - REDIS_MAX_CONNECTIONS: Maximum pool connections (default: 50)
 * - REDIS_MIN_IDLE_CONNECTIONS: Minimum idle connections (default: 5)
 * - REDIS_MAX_IDLE_CONNECTIONS: Maximum idle connections (default: 10)
 * - REDIS_CONNECTION_TIMEOUT_MS: Connection timeout in milliseconds (default: 5000)
 * - REDIS_SO_TIMEOUT_MS: Socket timeout in milliseconds (default: 2000)
 * - REDIS_MAX_REDIRECTIONS: Maximum cluster redirections (default: 5)
 */
class RedisApiImpl(conf: Map[String, String]) extends Api(conf) {
  import RedisApiImpl._

  @transient override lazy val logger: Logger = LoggerFactory.getLogger("RedisApiImpl")

  // Singleton KVStore instance (like GcpApiImpl)
  override def genKvStore: KVStore = {
    Option(sharedKvStore.get()) match {
      case Some(existingStore) => existingStore
      case None =>
        kvStoreLock.synchronized {
          Option(sharedKvStore.get()) match {
            case Some(existingStore) => existingStore
            case None =>
              val newStore = createKVStore()
              sharedKvStore.set(newStore)
              newStore
          }
        }
    }
  }

  private def createKVStore(): KVStore = {
    // Parse cluster nodes from environment or config
    val nodesStr = getOrElseThrow(RedisKVStoreConstants.EnvRedisClusterNodes, conf)
    val password = getOptional(RedisKVStoreConstants.EnvRedisPassword, conf)
    val maxConnections = getOptional(RedisKVStoreConstants.EnvRedisMaxConnections, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultMaxConnections)
    val minIdleConnections = getOptional(RedisKVStoreConstants.EnvRedisMinIdleConnections, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultMinIdleConnections)
    val maxIdleConnections = getOptional(RedisKVStoreConstants.EnvRedisMaxIdleConnections, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultMaxIdleConnections)
    val connectionTimeoutMs = getOptional(RedisKVStoreConstants.EnvRedisConnectionTimeoutMs, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultConnectionTimeoutMs)
    val soTimeoutMs = getOptional(RedisKVStoreConstants.EnvRedisSoTimeoutMs, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultSoTimeoutMs)
    val maxRedirections = getOptional(RedisKVStoreConstants.EnvRedisMaxRedirections, conf).map(_.toInt).getOrElse(RedisKVStoreConstants.DefaultMaxRedirections)

    // Parse cluster nodes: "node1:6379,node2:6379,node3:6379"
    val clusterNodes = nodesStr.split(",").map { node =>
      val parts = node.trim.split(":")
      if (parts.length == 2) {
        new HostAndPort(parts(0), parts(1).toInt)
      } else {
        new HostAndPort(parts(0), RedisKVStoreConstants.DefaultPort)
      }
    }.toSet

    logger.info(
      s"Creating Redis Cluster KVStore with nodes: ${clusterNodes.mkString(", ")}. " +
      s"Params: maxConnections=$maxConnections, minIdle=$minIdleConnections, " +
      s"maxIdle=$maxIdleConnections, connectionTimeout=$connectionTimeoutMs, " +
      s"soTimeout=$soTimeoutMs, maxRedirections=$maxRedirections")

    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(maxConnections)
    poolConfig.setMaxIdle(maxIdleConnections)
    poolConfig.setMinIdle(minIdleConnections)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)

    val jedisCluster = password match {
      case Some(pwd) =>
        new JedisCluster(clusterNodes.asJava, connectionTimeoutMs, soTimeoutMs, maxRedirections, pwd, poolConfig.asInstanceOf[org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]])
      case None =>
        new JedisCluster(clusterNodes.asJava, connectionTimeoutMs, soTimeoutMs, maxRedirections, poolConfig.asInstanceOf[org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]])
    }

    val keyPrefix = getOptional(RedisKVStoreConstants.EnvRedisKeyPrefix, conf).getOrElse(RedisKVStoreConstants.DefaultKeyPrefix)
    val kvStore = new RedisKVStoreImpl(jedisCluster, Map("redis.key.prefix" -> keyPrefix))
    kvStore.init()
    kvStore
  }

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe =
    new AvroSerDe(AvroConversions.fromChrononSchema(groupByServingInfoParsed.streamChrononSchema))

  // TODO: Load from user jar
  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  override def externalRegistry: ExternalSourceRegistry = registry

  override def genMetricsKvStore(tableBaseName: String): KVStore = {
    // Separate KVStore instance for data quality metrics
    // Provides independent connection pool and metrics tracking
    Option(sharedDataQualityKvStore.get()) match {
      case Some(existingStore) => existingStore
      case None =>
        dataQualityKvStoreLock.synchronized {
          Option(sharedDataQualityKvStore.get()) match {
            case Some(existingStore) => existingStore
            case None =>
              val newStore = createKVStore()
              sharedDataQualityKvStore.set(newStore)
              newStore
          }
        }
    }
  }

  override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = {
    // Separate KVStore instance for enhanced statistics
    // Data separation achieved via different dataset names (tableBaseName)
    Option(sharedEnhancedStatsKvStore.get()) match {
      case Some(existingStore) => existingStore
      case None =>
        enhancedStatsKvStoreLock.synchronized {
          Option(sharedEnhancedStatsKvStore.get()) match {
            case Some(existingStore) => existingStore
            case None =>
              val newStore = createKVStore()
              sharedEnhancedStatsKvStore.set(newStore)
              newStore
          }
        }
    }
  }

  override def logResponse(resp: LoggableResponse): Unit = {
    // Implement logging based on your requirements
    // Options:
    // 1. Kafka logging (like cloud_gcp with KafkaLoggableResponseConsumer)
    // 2. PubSub logging
    // 3. Redis-based logging
    // 4. No-op for now
    logger.debug(s"Logging response for join: ${resp.joinName}")
  }
}

object RedisApiImpl {
  // Singleton KVStore instances with thread-safe lazy initialization
  private val sharedKvStore = new AtomicReference[KVStore]()
  private val kvStoreLock = new Object()

  private val sharedDataQualityKvStore = new AtomicReference[KVStore]()
  private val dataQualityKvStoreLock = new Object()

  private val sharedEnhancedStatsKvStore = new AtomicReference[KVStore]()
  private val enhancedStatsKvStoreLock = new Object()

  private[redis] def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env.get(key).orElse(conf.get(key))

  private[redis] def getOrElseThrow(key: String, conf: Map[String, String]): String =
    getOptional(key, conf).getOrElse(
      throw new IllegalArgumentException(s"$key environment variable not set")
    )
}

