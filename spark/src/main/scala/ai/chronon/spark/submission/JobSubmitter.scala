package ai.chronon.spark.submission

import ai.chronon.api
import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.spark.submission.JobSubmitterConstants.ConfTypeArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.LocalConfPathArgKeyword
import ai.chronon.spark.submission.JobSubmitterConstants.OriginalModeArgKeyword
import ai.chronon.api.{JobStatusType, MetaData, ThriftJsonCodec}
import ai.chronon.api.thrift.TBase
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

sealed trait JobType
case object SparkJob extends JobType
case object FlinkJob extends JobType

trait JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def submit(jobType: JobType,
             submissionProperties: Map[String, String],
             jobProperties: Map[String, String],
             files: List[String],
             labels: Map[String, String],
             args: String*): String

  def status(jobId: String): JobStatusType

  def kill(jobId: String): Unit
}

object JobSubmitter {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def getArgValue(args: Array[String], argKeyword: String): Option[String] = {
    args
      .find(_.startsWith(argKeyword))
      .map(_.split("="))
      .map(_(1))
  }

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = false)

  def getMetadata(args: Array[String]): Option[MetaData] = {
    val localConfPathValue = getArgValue(args, LocalConfPathArgKeyword)
    val confTypeValue = getArgValue(args, ConfTypeArgKeyword)
    val originalMode = getArgValue(args, OriginalModeArgKeyword)

    if (localConfPathValue.isDefined) {
      val metadata = if (confTypeValue.isDefined) {
        confTypeValue.get match {
          case "joins"           => parseConf[api.Join](localConfPathValue.get).metaData
          case "group_bys"       => parseConf[api.GroupBy](localConfPathValue.get).metaData
          case "staging_queries" => parseConf[api.StagingQuery](localConfPathValue.get).metaData
          case "models"          => parseConf[api.Model](localConfPathValue.get).metaData
          case _ =>
            throw new IllegalArgumentException(
              s"Unable to retrieve object metadata due to invalid confType $confTypeValue"
            )
        }
      } else if (originalMode.isDefined && originalMode.get == "metastore") {
        // attempt to parse as a generic MetaData object
        parseConf[api.MetaData](localConfPathValue.get)
      } else {
        throw new IllegalArgumentException("Unable to retrieve object metadata")
      }

      Option(metadata)
    } else None
  }

  def getModeConfigProperties(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val modeConfigProperties = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.conf), originalMode) match {
          case (Some(conf), Some(mode)) =>
            val modeConfs = if (conf.isSetModeConfigs && conf.getModeConfigs.containsKey(mode)) {
              conf.getModeConfigs.get(mode).toScala
            } else if (conf.isSetCommon) {
              conf.getCommon.toScala
            } else {
              Map[String, String]()
            }
            Option(modeConfs)
          case _ => None
        }
      }
    } else None

    logger.info(s"Setting job properties: $modeConfigProperties")

    modeConfigProperties
  }

  def getClusterConfig(args: Array[String]): Option[Map[String, String]] = {
    val maybeMetadata = getMetadata(args)
    val clusterConfig = if (maybeMetadata.isDefined) {
      val metadata = maybeMetadata.get

      val executionInfo = Option(metadata.getExecutionInfo)

      if (executionInfo.isEmpty) {
        None
      } else {
        val originalMode = getArgValue(args, OriginalModeArgKeyword)

        (Option(executionInfo.get.clusterConf), originalMode) match {
          case (Some(clusterConf), Some(mode)) =>
            val modeConfig =
              if (clusterConf.isSetModeClusterConfigs && clusterConf.getModeClusterConfigs.containsKey(mode)) {
                clusterConf.getModeClusterConfigs.get(mode).toScala
              } else if (clusterConf.isSetCommon) {
                clusterConf.getCommon.toScala
              } else {
                Map[String, String]()
              }
            Option(modeConfig)
          case _ => None
        }
      }
    } else None
    clusterConfig
  }

}

abstract class JobAuth {
  def token(): Unit = {}
}

object JobSubmitterConstants {
  val MainClass = "mainClass"
  val JarURI = "jarUri"
  val FlinkMainJarURI = "flinkMainJarUri"
  val FlinkPubSubConnectorJarURI = "flinkPubSubConnectorJarUri"
  val AdditionalJars = "additionalJars"
  val SavepointUri = "savepointUri"
  val FlinkStateUri = "flinkStateUri"
  val FlinkCheckpointUri = "flinkCheckpointUri"

  val JobId = "jobId"

  // Only lowercase, numbers, and dashes allowed for key labels in Dataproc
  val JobType = "job-type"
  val MetadataName = "metadata-name"
  val ZiplineVersion = "zipline-version"

  val SparkJobType = "spark"
  val FlinkJobType = "flink"

  // EMR specific properties
  val ClusterInstanceCount = "clusterInstanceCount"
  val ClusterInstanceType = "clusterInstanceType"
  val ClusterIdleTimeout = "clusterIdleTimeout"
  val ClusterId = "jobFlowId"
  val ClusterName = "clusterName"
  val SubnetId = "subnetId"
  val SecurityGroupId = "securityGroupId"

  val JarUriArgKeyword = "--jar-uri"
  val JobTypeArgKeyword = "--job-type"
  val MainClassKeyword = "--main-class"
  val FlinkMainJarUriArgKeyword = "--flink-main-jar-uri"
  val FlinkPubSubJarUriArgKeyword = "--flink-pubsub-jar-uri"
  val AdditionalJarsUriArgKeyword = "--additional-jars"
  val FlinkGroupByNameArgKeyword = "--groupby-name"
  val FilesArgKeyword = "--files"
  val ConfTypeArgKeyword = "--conf-type"
  val ConfPathArgKeyword = "--conf-path"
  val LocalConfPathArgKeyword = "--local-conf-path"
  val OriginalModeArgKeyword = "--original-mode"
  val ZiplineVersionArgKeyword = "--zipline-version"
  val GroupByNameArgKeyword = "--groupby-name"
  val OnlineClassArgKeyword = "--online-class"
  val ApiPropsArgPrefix = "-Z"
  val LocalZiplineVersionArgKeyword = "--local-zipline-version"
  val StreamingManifestPathArgKeyword = "--streaming-manifest-path"
  val StreamingCheckpointPathArgKeyword = "--streaming-checkpoint-path"
  val StreamingModeArgKeyword = "--streaming-mode"

  val StreamingVersionCheckDeploy = "--version-check"

  val StreamingLatestSavepointArgKeyword = "--latest-savepoint"
  val StreamingCustomSavepointArgKeyword = "--custom-savepoint"
  val StreamingNoSavepointArgKeyword = "--no-savepoint"

  val JobIdArgKeyword = "--job-id"

  val SharedInternalArgs: Set[String] = Set(
    JarUriArgKeyword,
    JobTypeArgKeyword,
    MainClassKeyword,
    FlinkMainJarUriArgKeyword,
    FlinkPubSubJarUriArgKeyword,
    AdditionalJarsUriArgKeyword,
    LocalConfPathArgKeyword,
    OriginalModeArgKeyword,
    FilesArgKeyword,
    ZiplineVersionArgKeyword,
    LocalZiplineVersionArgKeyword,
    StreamingModeArgKeyword,
    StreamingLatestSavepointArgKeyword,
    StreamingCustomSavepointArgKeyword,
    StreamingNoSavepointArgKeyword,
    StreamingCheckpointPathArgKeyword,
    StreamingVersionCheckDeploy,
    JobIdArgKeyword
  )

  val GcpBigtableInstanceIdEnvVar = "GCP_BIGTABLE_INSTANCE_ID"
  val GcpProjectIdEnvVar = "GCP_PROJECT_ID"
  val GcpRegionEnvVar = "GCP_REGION"
  val GcpLocationEnvVar = "GCP_LOCATION"
  val GcpDataprocClusterNameEnvVar = "GCP_DATAPROC_CLUSTER_NAME"
  val GcpEnableUploadKVClientEnvVar = "ENABLE_UPLOAD_CLIENTS"

  val TablePartitionsDatasetNameArgKeyword = "--table-partitions-dataset"
  val TableStatsDatasetNameArgKeyword = "--table-stats-dataset"

  val CheckIfJobIsRunning = "check-if-job-is-running"
  val StreamingDeploy = "deploy"

  // We use incremental checkpoints and we cap how many we keep around
  val MaxRetainedCheckpoints: String = "10"
}
