package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.aws.EmrSubmitter.{
  DefaultClusterIdleTimeout,
  DefaultClusterInstanceCount,
  DefaultClusterInstanceType
}
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.{JobSubmitter, JobType, SparkJob => TypeSparkJob}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.{DescribeSecurityGroupsRequest, DescribeSubnetsRequest, Filter}
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model._

import scala.jdk.CollectionConverters._

class EmrSubmitter(customerId: String, emrClient: EmrClient, ec2Client: Ec2Client) extends JobSubmitter {

  private val ClusterApplications = List(
    "Flink",
    "Zeppelin",
    "JupyterEnterpriseGateway",
    "Hive",
    "Hadoop",
    "Livy",
    "Spark"
  )

  // TODO: test if this works for Flink
  private val DefaultEmrReleaseLabel = "emr-7.2.0"

  /** Looks up a subnet ID from a subnet name tag.
    *
    * @param subnetName The value of the Name tag for the subnet
    * @return The subnet ID
    */
  private def lookupSubnetIdFromName(subnetName: String): String = {
    try {
      val request = DescribeSubnetsRequest
        .builder()
        .filters(
          Filter.builder().name("tag:Name").values(subnetName).build()
        )
        .build()

      val response = ec2Client.describeSubnets(request)
      val subnets = response.subnets().asScala

      if (subnets.isEmpty) {
        throw new RuntimeException(s"No subnet found with Name tag: $subnetName")
      }
      if (subnets.size > 1) {
        logger.warn(s"Multiple subnets found with Name tag: $subnetName. Using the first one.")
      }

      val subnetId = subnets.head.subnetId()
      logger.info(s"Resolved subnet name '$subnetName' to subnet ID: $subnetId")
      subnetId
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error looking up subnet ID for name '$subnetName': ${e.getMessage}", e)
    }
  }

  /** Looks up a security group ID from a security group name.
    *
    * @param securityGroupName The name of the security group
    * @return The security group ID
    */
  private def lookupSecurityGroupIdFromName(securityGroupName: String): String = {
    try {
      val request = DescribeSecurityGroupsRequest
        .builder()
        .filters(
          Filter.builder().name("group-name").values(securityGroupName).build()
        )
        .build()

      val response = ec2Client.describeSecurityGroups(request)
      val securityGroups = response.securityGroups().asScala

      if (securityGroups.isEmpty) {
        throw new RuntimeException(s"No security group found with name: $securityGroupName")
      }
      if (securityGroups.size > 1) {
        logger.warn(s"Multiple security groups found with name: $securityGroupName. Using the first one.")
      }

      val securityGroupId = securityGroups.head.groupId()
      logger.info(s"Resolved security group name '$securityGroupName' to security group ID: $securityGroupId")
      securityGroupId
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error looking up security group ID for name '$securityGroupName': ${e.getMessage}",
                                   e)
    }
  }

  private def createClusterRequestBuilder(emrReleaseLabel: String = DefaultEmrReleaseLabel,
                                          clusterIdleTimeout: Int = DefaultClusterIdleTimeout,
                                          masterInstanceType: String = DefaultClusterInstanceType,
                                          slaveInstanceType: String = DefaultClusterInstanceType,
                                          instanceCount: Int = DefaultClusterInstanceCount,
                                          clusterName: Option[String] = None,
                                          subnetId: Option[String] = None,
                                          securityGroupId: Option[String] = None) = {
    val runJobFlowRequestBuilder = if (clusterName.isDefined) {
      RunJobFlowRequest
        .builder()
        .name(clusterName.get)
    } else {
      RunJobFlowRequest
        .builder()
        .name(s"job-${java.util.UUID.randomUUID.toString}")
    }

    // Cluster infra configurations:
    val finalSubnetId =
      subnetId.getOrElse(throw new RuntimeException(s"Subnet ID must be provided in cluster configuration"))
    val finalSecurityGroupId = securityGroupId.getOrElse(
      throw new RuntimeException(s"Security group ID must be provided in cluster configuration"))

    runJobFlowRequestBuilder
      .autoTerminationPolicy(
        AutoTerminationPolicy
          .builder()
          .idleTimeout(clusterIdleTimeout.toLong)
          .build())
      .configurations(
        Configuration.builder
          .classification("spark-hive-site")
          .properties(Map(
            "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").asJava)
          .build()
      )
      .applications(ClusterApplications.map(app => Application.builder().name(app).build()): _*)
      // TODO: Could make this generalizable. or use a separate logs bucket
      .logUri(s"s3://zipline-logs-${customerId}/emr/")
      .instances(
        JobFlowInstancesConfig
          .builder()
          .ec2SubnetId(finalSubnetId)
          .emrManagedMasterSecurityGroup(finalSecurityGroupId)
          .emrManagedSlaveSecurityGroup(finalSecurityGroupId)
          .instanceGroups(
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.MASTER)
              .instanceType(masterInstanceType)
              .instanceCount(1)
              .build(),
            InstanceGroupConfig
              .builder()
              .instanceRole(InstanceRoleType.CORE)
              .instanceType(slaveInstanceType)
              .instanceCount(1)
              .build()
          )
          .keepJobFlowAliveWhenNoSteps(true) // Keep the cluster alive after the job is done
          .build())
      .managedScalingPolicy(
        ManagedScalingPolicy
          .builder()
          .computeLimits(
            ComputeLimits
              .builder()
              .maximumCapacityUnits(instanceCount)
              .minimumCapacityUnits(1)
              .unitType(ComputeLimitsUnitType.INSTANCES)
              .build()
          )
          .build()
      )
      .serviceRole(s"zipline_${customerId}_emr_service_role")
      .jobFlowRole(s"zipline_${customerId}_emr_profile_role")
      .releaseLabel(emrReleaseLabel)

  }

  private def createStepConfig(filesToMount: List[String],
                               mainClass: String,
                               jarUri: String,
                               args: String*): StepConfig = {
    // TODO: see if we can use the spark.files or --files instead of doing this ourselves
    // Copy files from s3 to cluster
    val awsS3CpArgs = filesToMount.map(file => s"aws s3 cp $file /mnt/zipline/")
    val sparkSubmitArgs =
      List(s"spark-submit --class $mainClass $jarUri ${args.mkString(" ")}")
    val finalArgs = List(
      "bash",
      "-c",
      (awsS3CpArgs ++ sparkSubmitArgs).mkString("; \n")
    )
    logger.debug(s"Step config args: $finalArgs")
    StepConfig
      .builder()
      .name("Run Zipline Job")
      .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
      .hadoopJarStep(
        HadoopJarStepConfig
          .builder()
          // Using command-runner.jar from AWS:
          // https://docs.aws.amazon.com/en_us/emr/latest/ReleaseGuide/emr-spark-submit-step.html
          .jar("command-runner.jar")
          .args(finalArgs: _*)
          .build()
      )
      .build()
  }

  /** Gets or creates an EMR cluster with the given configuration.
    * Similar to DataprocSubmitter.getOrCreateCluster.
    *
    * @param clusterName The name of the cluster
    * @param maybeClusterConfig Optional cluster configuration map
    * @return The cluster ID
    */
  def getOrCreateCluster(clusterName: String, maybeClusterConfig: Option[Map[String, String]]): String = {
    require(clusterName.nonEmpty, "clusterName cannot be empty")

    try {
      // Try to find the cluster by name
      val listClustersResponse = emrClient.listClusters(
        ListClustersRequest
          .builder()
          .clusterStates(
            ClusterState.STARTING,
            ClusterState.BOOTSTRAPPING,
            ClusterState.RUNNING,
            ClusterState.WAITING,
            ClusterState.TERMINATED
          )
          .build()
      )

      val matchingCluster: Option[ClusterSummary] = listClustersResponse
        .clusters()
        .asScala
        .find(_.name() == clusterName)

      matchingCluster match {
        case Some(cluster) if Set(ClusterState.RUNNING, ClusterState.WAITING).contains(cluster.status().state()) =>
          logger.info(s"EMR cluster $clusterName already exists and is in state ${cluster.status().state()}.")
          cluster.id()

        case Some(cluster) if cluster.status().state() == ClusterState.TERMINATED =>
          val stateChangeReason = cluster.status().stateChangeReason()
          val terminationMessage = if (stateChangeReason != null && stateChangeReason.message() != null) {
            stateChangeReason.message()
          } else {
            "No termination reason provided"
          }
          logger.error(s"EMR cluster $clusterName is TERMINATED. Reason: $terminationMessage")
          throw new RuntimeException(
            s"EMR cluster $clusterName is in TERMINATED state and cannot be used. " +
              s"Termination reason: $terminationMessage. Please create a new cluster or check cluster configuration."
          )

        case Some(cluster) =>
          logger.info(
            s"EMR cluster $clusterName exists but is in state ${cluster.status().state()}. Waiting for it to be ready.")
          waitForClusterReadiness(cluster.id(), clusterName)

        case None =>
          // Cluster doesn't exist, create it if config is provided
          if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("emr.config")) {
            logger.info(s"EMR cluster $clusterName does not exist. Creating it with the provided config.")

            createEmrCluster(clusterName, maybeClusterConfig.get.getOrElse("emr.config", ""))
          } else {
            throw new Exception(s"EMR cluster $clusterName does not exist and no cluster config provided.")
          }
      }
    } catch {
      case e: EmrException =>
        if (maybeClusterConfig.isDefined && maybeClusterConfig.get.contains("emr.config")) {
          logger.info(s"Error checking for EMR cluster. Creating it with the provided config.")

          createEmrCluster(clusterName, maybeClusterConfig.get.getOrElse("emr.config", ""))
        } else {
          throw new Exception(s"EMR cluster $clusterName does not exist and no cluster config provided.", e)
        }
    }
  }

  /** Waits for an EMR cluster to reach a ready state (RUNNING or WAITING).
    * Fails fast if the cluster transitions to TERMINATED state.
    *
    * @param clusterId The ID of the cluster to wait for
    * @param clusterName The name of the cluster (for logging)
    * @return The cluster ID if successful
    */
  private def waitForClusterReadiness(clusterId: String, clusterName: String): String = {
    val maxWaitTimeMs = 600000 // 10 minutes
    val pollIntervalMs = 10000 // 10 seconds
    val startTime = System.currentTimeMillis()

    logger.info(s"Waiting for EMR cluster $clusterName (ID: $clusterId) to be ready...")

    while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
      val describeClusterRequest = DescribeClusterRequest.builder().clusterId(clusterId).build()
      val clusterDetails = emrClient.describeCluster(describeClusterRequest).cluster()
      val currentState = clusterDetails.status().state()

      currentState match {
        case ClusterState.RUNNING | ClusterState.WAITING =>
          logger.info(s"EMR cluster $clusterName is now ready in state: $currentState")
          return clusterId

        case ClusterState.TERMINATED | ClusterState.TERMINATING | ClusterState.TERMINATED_WITH_ERRORS =>
          val stateChangeReason = clusterDetails.status().stateChangeReason()
          val terminationMessage = if (stateChangeReason != null && stateChangeReason.message() != null) {
            stateChangeReason.message()
          } else {
            "No termination reason provided"
          }
          logger.error(s"EMR cluster $clusterName entered terminal state $currentState. Reason: $terminationMessage")
          throw new RuntimeException(
            s"EMR cluster $clusterName (ID: $clusterId) entered terminal state: $currentState. " +
              s"Termination reason: $terminationMessage. Cannot proceed with job submission."
          )

        case ClusterState.STARTING | ClusterState.BOOTSTRAPPING =>
          logger.info(s"EMR cluster $clusterName is in state $currentState, waiting...")
          Thread.sleep(pollIntervalMs)

        case _ =>
          logger.warn(s"EMR cluster $clusterName is in unexpected state: $currentState")
          Thread.sleep(pollIntervalMs)
      }
    }

    throw new RuntimeException(
      s"Timeout waiting for EMR cluster $clusterName (ID: $clusterId) to be ready after ${maxWaitTimeMs / 1000} seconds"
    )
  }

  /** Creates an EMR cluster with the given configuration.
    *
    * @param clusterName The name of the cluster to create
    * @param clusterConfigStr JSON string representing the cluster configuration
    * @return The cluster ID
    */
  private def createEmrCluster(clusterName: String, clusterConfigStr: String): String = {
    try {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val configNode = mapper.readTree(clusterConfigStr)

      // Parse the EMR configuration from JSON
      val emrReleaseLabel = Option(configNode.get("releaseLabel"))
        .map(_.asText())
        .getOrElse(DefaultEmrReleaseLabel)

      val idleTimeout = Option(configNode.get("autoTerminationPolicy"))
        .flatMap(atp => Option(atp.get("idleTimeout")))
        .map(_.asInt())
        .getOrElse(DefaultClusterIdleTimeout)

      val instanceType = Option(configNode.get("instanceType"))
        .map(_.asText())
        .getOrElse(DefaultClusterInstanceType)

      val instanceCount = Option(configNode.get("instanceCount"))
        .map(_.asInt())
        .getOrElse(DefaultClusterInstanceCount)

      // Handle subnet - look up ID from name if provided
      val subnetId = Option(configNode.get("subnetName"))
        .map(_.asText())
        .map(lookupSubnetIdFromName)
        .orElse(Option(configNode.get("subnetId")).map(_.asText()))

      // Handle security group - look up ID from name if provided
      val securityGroupId = Option(configNode.get("securityGroupName"))
        .map(_.asText())
        .map(lookupSecurityGroupIdFromName)
        .orElse(Option(configNode.get("securityGroupId")).map(_.asText()))

      // Create the cluster using the existing builder
      val runJobFlowBuilder = createClusterRequestBuilder(
        emrReleaseLabel = emrReleaseLabel,
        clusterIdleTimeout = idleTimeout,
        masterInstanceType = instanceType,
        slaveInstanceType = instanceType,
        instanceCount = instanceCount,
        clusterName = Some(clusterName),
        subnetId = subnetId,
        securityGroupId = securityGroupId
      )

      val response = emrClient.runJobFlow(runJobFlowBuilder.build())
      val clusterId = response.jobFlowId()

      logger.info(s"Created EMR cluster: $clusterName with ID: $clusterId")
      clusterId

    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error creating EMR cluster $clusterName: ${e.getMessage}", e)
    }
  }

  override def submit(jobType: JobType,
                      submissionProperties: Map[String, String],
                      jobProperties: Map[String, String],
                      files: List[String],
                      labels: Map[String, String],
                      args: String*): String = {
    val existingJobId = submissionProperties.getOrElse(ClusterId, throw new RuntimeException("JobFlowId not found"))
    val userArgs = args.filter(arg => !SharedInternalArgs.exists(arg.startsWith))
    val request = AddJobFlowStepsRequest
      .builder()
      .jobFlowId(existingJobId)
      .steps(createStepConfig(files, submissionProperties(MainClass), submissionProperties(JarURI), userArgs: _*))
      .build()

    val responseStepId = emrClient.addJobFlowSteps(request).stepIds().get(0)

    logger.info(s"EMR step id: $responseStepId")
    logger.info(
      s"Safe to exit. Follow the job status at: https://console.aws.amazon.com/emr/home#/clusterDetails/$existingJobId")
    responseStepId
  }

  override def status(jobId: String): JobStatusType = ???

  override def kill(stepId: String): scala.Unit = {
    val resp = emrClient.cancelSteps(CancelStepsRequest.builder().stepIds(stepId).build())
  }
}

object EmrSubmitter {
  def apply(): EmrSubmitter = {
    val customerId = sys.env.getOrElse("CUSTOMER_ID", throw new Exception("CUSTOMER_ID not set")).toLowerCase

    new EmrSubmitter(
      customerId,
      EmrClient.builder().build(),
      Ec2Client.builder().build()
    )
  }

  private val ClusterInstanceTypeArgKeyword = "--cluster-instance-type"
  private val ClusterInstanceCountArgKeyword = "--cluster-instance-count"
  private val ClusterIdleTimeoutArgKeyword = "--cluster-idle-timeout"

  private val DefaultClusterInstanceType = "m5.xlarge"
  private val DefaultClusterInstanceCount = 3
  private val DefaultClusterIdleTimeout = 60 * 60 * 1 // 1h in seconds

  def main(args: Array[String]): scala.Unit = {
    // List of args that are not application args
    val internalArgs = Set(
      ClusterInstanceTypeArgKeyword,
      ClusterInstanceCountArgKeyword,
      ClusterIdleTimeoutArgKeyword
    ) ++ SharedInternalArgs

    val userArgs = args.filter(arg => !internalArgs.exists(arg.startsWith))

    val jarUri = JobSubmitter
      .getArgValue(args, JarUriArgKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + JarUriArgKeyword))
    val mainClass = JobSubmitter
      .getArgValue(args, MainClassKeyword)
      .getOrElse(throw new Exception("Missing required argument: " + MainClassKeyword))
    val jobTypeValue =
      JobSubmitter
        .getArgValue(args, JobTypeArgKeyword)
        .getOrElse(throw new Exception("Missing required argument: " + JobTypeArgKeyword))

    val clusterInstanceType = JobSubmitter
      .getArgValue(args, ClusterInstanceTypeArgKeyword)
      .getOrElse(DefaultClusterInstanceType)
    val clusterInstanceCount = JobSubmitter
      .getArgValue(args, ClusterInstanceCountArgKeyword)
      .getOrElse(DefaultClusterInstanceCount.toString)
    val clusterIdleTimeout = JobSubmitter
      .getArgValue(args, ClusterIdleTimeoutArgKeyword)
      .getOrElse(DefaultClusterIdleTimeout.toString)

    val clusterId = sys.env.get("EMR_CLUSTER_ID")

    // search args array for prefix `--gcs_files`
    val filesArgs = args.filter(_.startsWith(FilesArgKeyword))
    assert(filesArgs.length == 0 || filesArgs.length == 1)

    val files = if (filesArgs.isEmpty) {
      Array.empty[String]
    } else {
      filesArgs(0).split("=")(1).split(",")
    }

    val (jobType, submissionProps) = jobTypeValue.toLowerCase match {
      case "spark" => {
        val baseProps = Map(
          MainClass -> mainClass,
          JarURI -> jarUri,
          ClusterInstanceType -> clusterInstanceType,
          ClusterInstanceCount -> clusterInstanceCount,
          ClusterIdleTimeout -> clusterIdleTimeout
        )

        (TypeSparkJob, baseProps + (ClusterId -> clusterId.get))
      }
      // TODO: add flink
      case _ => throw new Exception("Invalid job type")
    }

    val finalArgs = userArgs.toSeq
    val modeConfigProperties = JobSubmitter.getModeConfigProperties(args)

    val emrSubmitter = EmrSubmitter()
    emrSubmitter.submit(
      jobType = jobType,
      submissionProperties = submissionProps,
      jobProperties = modeConfigProperties.getOrElse(Map.empty),
      files = files.toList,
      labels = Map.empty,
      finalArgs: _*
    )
  }
}
