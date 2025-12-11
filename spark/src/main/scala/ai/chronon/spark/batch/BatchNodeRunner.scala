package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.{DependencyResolver, NodeRunner}
import ai.chronon.observability.{TileStats, TileStatsType}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.{Api, KVStore}
import ai.chronon.planner._
import ai.chronon.spark.batch.iceberg.IcebergPartitionStatsExtractor
import ai.chronon.spark.batch.{StagingQuery => StagingQueryUtil}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.join.UnionJoin
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.{GroupBy, GroupByUpload, Join, ModelTransformsJob}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class BatchNodeRunnerArgs(args: Array[String]) extends ScallopConf(args) {

  this: ScallopConf =>

  val confPath: ScallopOption[String] = opt[String](required = true, descr = "Path to node configuration file")
  val startDs: ScallopOption[String] = opt[String](
    required = false,
    descr = "Start date string in format yyyy-MM-dd, used for partitioning"
  )
  val endDs: ScallopOption[String] = opt[String](
    required = true,
    descr = "End date string in format yyyy-MM-dd, used for partitioning"
  )

  val onlineClass: ScallopOption[String] = opt[String](
    required = true,
    descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

  val apiProps: Map[String, String] = props[String]('Z', descr = "Props to configure API Store")

  val tablePartitionsDataset: ScallopOption[String] = opt[String](
    required = true,
    descr = "Name of table in kv store to use to keep track of partitions",
    default = Option(NodeRunner.DefaultTablePartitionsDataset))

  val tableStatsDataset: ScallopOption[String] = opt[String](
    required = false,
    descr = "Name of table in kv store to use to store partition statistics",
    default = None
  )

  verify()
}

class BatchNodeRunner(node: Node, tableUtils: TableUtils, api: Api) extends NodeRunner {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def checkPartitions(conf: ExternalSourceSensorNode, range: PartitionRange): Try[Unit] = {
    val tableName = Option(conf.sourceTableDependency)
      .map(_.tableInfo)
      .map(_.table)
      .getOrElse(
        throw new IllegalArgumentException("ExternalSourceSensorNode must have a sourceTableDependency defined")
      )
    val retryCount = if (conf.isSetRetryCount) conf.retryCount else 3L
    val retryIntervalMin = if (conf.isSetRetryIntervalMin) conf.retryIntervalMin else 3L

    val tableInfo = conf.sourceTableDependency.tableInfo
    val hasPartitionColumn = Option(tableInfo.partitionColumn).isDefined
    val hasTriggerExpr = Option(tableInfo.triggerExpr).isDefined

    // Case 1: No partitions or trigger expression defined -> return success
    if (!hasPartitionColumn && !hasTriggerExpr) {
      logger.info(
        s"Input table ${tableName} has no partitions or trigger expression defined. Checking table existence.")
      if (tableUtils.tableReachable(tableName)) return Success(())
      else return Failure(new RuntimeException(s"Table ${tableName} was not found."))
    }

    // Case 2: No partitions but trigger expression is defined
    if (!hasPartitionColumn && hasTriggerExpr) {
      val triggerExpr = tableInfo.triggerExpr
      @tailrec
      def retryTriggerExpr(attempt: Long): Try[Unit] = {
        Try {
          val sql = s"SELECT ${triggerExpr} FROM ${tableName}"
          logger.info(s"Executing trigger expression query: ${sql} on engine: ${conf.engineType.name()}")
          val result = conf.engineType match {
            case EngineType.SPARK => tableUtils.sql(sql)
            // TODO: Implement EngineType.BIG_QUERY (Possibly through tableUtils)
            case _ => throw new RuntimeException("Not implemented.")
          }
          val triggerValue = result
            .collect()
            .headOption
            .getOrElse(throw new RuntimeException(s"Trigger expression query returned no results"))
            .get(0)

          // Compare trigger value against partitionRange.max (end partition)
          val maxPartition = range.end

          // Convert both to comparable format - assuming the trigger expression returns a comparable value
          val triggerValueStr = triggerValue.toString
          logger.info(s"Trigger value: ${triggerValueStr}, Max partition: ${maxPartition}")

          if (triggerValueStr > maxPartition) {
            logger.info(
              s"Trigger expression ${triggerExpr} value ${triggerValueStr} > ${maxPartition}. Sensor succeeded.")
            ()
          } else {
            throw new RuntimeException(
              s"Trigger expression ${triggerExpr} value ${triggerValueStr} is not greater than ${maxPartition}")
          }
        } match {
          case Success(_) => Success(())
          case Failure(e) if attempt < retryCount =>
            logger.warn(
              s"Attempt ${attempt + 1} failed: Trigger expression check failed with error: ${e.getMessage}. " +
                s"Retrying in ${retryIntervalMin} minutes")
            Thread.sleep(retryIntervalMin * 60 * 1000)
            retryTriggerExpr(attempt + 1)
          case Failure(e) =>
            Failure(
              new RuntimeException(s"Sensor timed out after ${retryIntervalMin * attempt} minutes. " +
                                     s"Trigger expression check failed: ${e.getMessage}",
                                   e))
        }
      }
      return retryTriggerExpr(0)
    }

    // Case 3: Use existing partition check logic
    val spec = tableInfo.partitionSpec(tableUtils.partitionSpec)

    @tailrec
    def retry(attempt: Long): Try[Unit] = {
      val result = Try {
        val partitionsInRange =
          tableUtils.partitions(tableName,
                                partitionRange = Option(range.translate(spec)),
                                tablePartitionSpec = Option(spec))
        Option(range).map(_.partitions).getOrElse(Seq.empty).diff(partitionsInRange)
      }

      result match {
        case Success(missingPartitions) if missingPartitions.isEmpty =>
          logger.info(s"Input table ${tableName} has the requested range present: ${range}.")
          Success(())
        case Success(missingPartitions) if attempt < retryCount =>
          logger.warn(
            s"Attempt ${attempt + 1} failed: Input table ${tableName} is missing partitions: ${missingPartitions
                .mkString(", ")}. Retrying in ${retryIntervalMin} minutes")
          Thread.sleep(retryIntervalMin * 60 * 1000)
          retry(attempt + 1)
        case Success(missingPartitions) =>
          Failure(new RuntimeException(
            s"Sensor timed out after ${retryIntervalMin * attempt} minutes. Input table ${tableName} is missing partitions: ${missingPartitions
                .mkString(", ")}"))
        case Failure(e) => Failure(e)
      }
    }
    retry(0)
  }

  private def runStagingQuery(metaData: MetaData, stagingQuery: StagingQueryNode, range: PartitionRange): Unit = {
    require(stagingQuery.isSetStagingQuery, "StagingQueryNode must have a stagingQuery set")
    logger.info(s"Running staging query for '${metaData.name}'")
    val stagingQueryConf = stagingQuery.stagingQuery
    val sq = StagingQueryUtil.from(stagingQueryConf, range.end, tableUtils)
    sq.compute(
      range,
      Option(stagingQuery.stagingQuery.setups).map(_.asScala.toSeq).getOrElse(Seq.empty),
      Option(true)
    )

    logger.info(s"Successfully completed staging query for '${metaData.name}'")
  }

  private def runGroupByUpload(metadata: MetaData, groupByUpload: GroupByUploadNode, range: PartitionRange): Unit = {
    require(groupByUpload.isSetGroupBy, "GroupByUploadNode must have a groupBy set")
    val groupBy = groupByUpload.groupBy
    logger.info(s"Running groupBy upload for '${metadata.name}' for day: ${range.end}")

    GroupByUpload.run(groupBy, range.end, Option(tableUtils))
    logger.info(s"Successfully completed groupBy upload for '${metadata.name}' for day: ${range.end}")
  }

  private def runMonolithJoin(metadata: MetaData, monolithJoin: MonolithJoinNode, range: PartitionRange): Unit = {
    require(monolithJoin.isSetJoin, "MonolithJoinNode must have a join set")

    val joinConf = monolithJoin.join
    val joinName = metadata.name

    val standaloneUnionJoinEligible = UnionJoin.isEligibleForStandaloneRun(joinConf)

    logger.info(
      s"Running join backfill for '$joinName' with skewFreeMode: ${tableUtils.skewFreeMode}, standalone union-join eligible: $standaloneUnionJoinEligible")
    logger.info(s"Processing range: [${range.start}, ${range.end}]")

    if (standaloneUnionJoinEligible && tableUtils.skewFreeMode) {

      logger.info(s"Using standalone-union-join. Will skip writing join-part table & source table.")

      UnionJoin.computeJoinAndSave(joinConf, range)(tableUtils)

      logger.info(s"Successfully wrote range: $range")

    } else {
      val join = new Join(joinConf, range.end, tableUtils)
      val result = join.forceComputeRangeAndSave(range)

      result match {
        case Some(df) =>
          logger.info(s"\nShowing three rows of output above.\nQuery table '${metadata.outputTable}' for more.\n")
          df.show(numRows = 3, truncate = 0, vertical = true)

        case None =>
          throw new IllegalArgumentException(
            s"Join produced no results for range $range. Ensure that the input data is all present"
          )
      }
    }
  }

  private[batch] def extractAndPersistPartitionStats(metricsKvStore: KVStore, outputTable: String, confName: String)(
      implicit partitionSpec: PartitionSpec): Unit = {
    try {
      logger.info(s"Extracting partition statistics for table: $outputTable")
      val statsExtractor = new IcebergPartitionStatsExtractor(tableUtils.sparkSession)

      statsExtractor.extractPartitionedStats(outputTable, confName) match {
        case Some(tileSummaries) if tileSummaries.nonEmpty =>
          val groupedTileSummaries = tileSummaries.groupBy { case (observabilityTileKey, _) =>
            val dayPartitionMillis =
              IcebergPartitionStatsExtractor.extractPartitionMillisFromSlice(observabilityTileKey.getSlice,
                                                                             partitionSpec)
            (dayPartitionMillis)
          }

          val statsPutRequests = groupedTileSummaries.map { case ((dayPartitionMillis), columnTileSummaries) =>
            val nullCountsStats = IcebergPartitionStatsExtractor.createNullCountsStats(columnTileSummaries)
            val partitionStats = TileStats.nullCounts(nullCountsStats)
            IcebergPartitionStatsExtractor.createPartitionStatsPutRequest(outputTable,
                                                                          partitionStats,
                                                                          dayPartitionMillis,
                                                                          TileStatsType.NULL_COUNTS)
          }.toSeq

          statsExtractor.extractSchemaMapping(outputTable) match {
            case Some(schemaMapping) =>
              val schemaPutRequest =
                IcebergPartitionStatsExtractor.createSchemaMappingPutRequest(outputTable, schemaMapping)
              val allPutRequests = statsPutRequests :+ schemaPutRequest

              try {
                val kvStoreUpdates = metricsKvStore.multiPut(allPutRequests)
                Await.result(kvStoreUpdates, 30.seconds)

                logger.info(
                  s"Successfully persisted data quality metrics and schema mapping for table: $outputTable (${tileSummaries.size} tile summaries)")
              } catch {
                case e: Exception =>
                  logger.info(
                    s"Failed to persist data quality metrics to KV store for table: $outputTable. This may be expected if the KV store table does not exist. Error: ${e.traceString}")
              }
            case None =>
              logger.info(
                s"Could not extract schema mapping for table: $outputTable, skipping column stats persistence")
          }
        case Some(tileSummaries) if tileSummaries.isEmpty =>
          logger.info(s"No tile summaries found for table: $outputTable")
        case None =>
          logger.info(
            s"Table $outputTable is not an Iceberg table or is not partitioned, skipping column stats extraction")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to extract/persist data quality metrics for table: $outputTable", e)
      // Don't fail the job if stats extraction fails
    }
  }

  override def run(metadata: MetaData, conf: NodeContent, maybeRange: Option[PartitionRange]): Unit = {
    require(maybeRange.isDefined, "Partition range must be defined for batch node runner")
    val range = maybeRange.get
    val dateRange = new DateRange().setStartDate(range.start).setEndDate(range.end)

    conf.getSetField match {
      case NodeContent._Fields.MONOLITH_JOIN =>
        runMonolithJoin(metadata, conf.getMonolithJoin, range)

      case NodeContent._Fields.UNION_JOIN =>
        logger.info(s"Running union join for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        require(conf.getUnionJoin.isSetJoin, "UnionJoinNode must have a join set")
        UnionJoin.computeJoinAndSave(conf.getUnionJoin.join, range)(tableUtils)
        logger.info(s"Successfully completed union join for '${metadata.name}'")

      case NodeContent._Fields.SOURCE_WITH_FILTER =>
        logger.info(s"Running source with filter job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new SourceJob(conf.getSourceWithFilter, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed source with filter job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_BOOTSTRAP =>
        logger.info(s"Running join bootstrap job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinBootstrapJob(conf.getJoinBootstrap, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed join bootstrap job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_PART =>
        logger.info(s"Running join part job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinPartJob(conf.getJoinPart, metadata, dateRange, alignOutput = true)(tableUtils).run()
        logger.info(s"Successfully completed join part job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_MERGE =>
        logger.info(s"Running join merge job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        val joinParts = Option(conf.getJoinMerge.join.joinParts).map(_.asScala.toSeq).getOrElse(Seq.empty)
        new MergeJob(conf.getJoinMerge, metadata, dateRange, joinParts)(tableUtils).run()
        logger.info(s"Successfully completed join merge job for '${metadata.name}'")

      case NodeContent._Fields.JOIN_DERIVATION =>
        logger.info(s"Running join derivation job for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        new JoinDerivationJob(conf.getJoinDerivation, metadata, dateRange)(tableUtils).run()
        logger.info(s"Successfully completed join derivation job for '${metadata.name}'")

      case NodeContent._Fields.GROUP_BY_UPLOAD =>
        runGroupByUpload(metadata, conf.getGroupByUpload, range)

      case NodeContent._Fields.GROUP_BY_BACKFILL =>
        logger.info(s"Running groupBy backfill for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        GroupBy.computeBackfill(
          conf.getGroupByBackfill.groupBy,
          range.end,
          tableUtils,
          overrideStartPartition = Option(range.start)
        )
        logger.info(s"Successfully completed groupBy backfill for '${metadata.name}'")

      case NodeContent._Fields.STAGING_QUERY =>
        runStagingQuery(metadata, conf.getStagingQuery, range)

      case NodeContent._Fields.EXTERNAL_SOURCE_SENSOR =>
        checkPartitions(conf.getExternalSourceSensor, range) match {
          case Success(_) =>
          case Failure(exception) =>
            logger.error(s"ExternalSourceSensor check failed.", exception)
            throw exception
        }

      case NodeContent._Fields.MODEL_TRANSFORMS_BACKFILL =>
        logger.info(
          s"Running model transforms backfill for '${metadata.name}' for range: [${range.start}, ${range.end}]")
        require(conf.getModelTransformsBackfill.isSetModelTransforms,
                "ModelTransformsBackfillNode must have modelTransforms set")
        val modelTransforms = conf.getModelTransformsBackfill.modelTransforms

        val modelPlatformProvider = Option(api.generateModelPlatformProvider)
          .getOrElse(
            throw new IllegalStateException("Api with ModelPlatformProvider must be set for ModelTransforms backfill"))

        ModelTransformsJob.computeBackfill(
          modelTransforms,
          range,
          tableUtils,
          modelPlatformProvider
        )
        logger.info(s"Successfully completed model transforms backfill for '${metadata.name}'")

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported NodeContent type: ${conf.getSetField}")
    }
  }

  private def postJobActions(metadata: MetaData,
                             range: PartitionRange,
                             tablePartitionsDataset: String,
                             kvStore: KVStore,
                             tableStatsDataset: Option[String]): Unit = {
    val outputTablePartitionSpec = (for {
      meta <- Option(metadata)
      executionInfo <- Option(meta.executionInfo)
      outputTableInfo <- Option(executionInfo.outputTableInfo)
      definedSpec = outputTableInfo.partitionSpec(tableUtils.partitionSpec)
    } yield definedSpec).getOrElse(tableUtils.partitionSpec)
    val allOutputTablePartitions = tableUtils.partitions(metadata.executionInfo.outputTableInfo.table,
                                                         tablePartitionSpec = Option(outputTablePartitionSpec))

    val outputTablePartitionsJson = PartitionRange.collapsedPrint(allOutputTablePartitions)(range.partitionSpec)
    logger.info(s"Output table partitions for '${metadata.name}': $outputTablePartitionsJson")

    // Check if range is inside allOutputTablePartitions
    if (!range.partitions.forall(p => allOutputTablePartitions.contains(p))) {
      val missingPartitions = range.partitions.filterNot(p => allOutputTablePartitions.contains(p))
      logger.error(
        s"After job completion, output table ${metadata.executionInfo.outputTableInfo.table} is missing partitions: ${missingPartitions
            .mkString(", ")} from the requested range: $range. All output partitions: ${allOutputTablePartitions}"
      )
    }

    val putRequest = PutRequest(metadata.executionInfo.outputTableInfo.table.getBytes,
                                outputTablePartitionsJson.getBytes,
                                tablePartitionsDataset)
    val kvStoreUpdates = kvStore.put(putRequest)
    Await.result(kvStoreUpdates, Duration.Inf)
    logger.info(s"Successfully completed batch node runner for '${metadata.name}'")

    // Extract and persist partition statistics to KV store - done at the very end
    // Skip data quality metrics persistence for EXTERNAL_SOURCE_SENSOR nodes
    if (node.content.getSetField != NodeContent._Fields.EXTERNAL_SOURCE_SENSOR) {
      tableStatsDataset.foreach { tableStats =>
        val metricsKvStore = api.genMetricsKvStore(tableStats)
        Option(metadata.outputTable) match {
          case Some(outputTable) =>
            extractAndPersistPartitionStats(metricsKvStore, outputTable, metadata.name)(outputTablePartitionSpec)
          case None =>
            logger.warn(s"Skipping partition stats extraction for '${metadata.name}' - outputTable is null")
        }
      }
    } else {
      logger.info(s"Skipping data quality metrics persistence for EXTERNAL_SOURCE_SENSOR node '${metadata.name}'")
    }
  }

  def runFromArgs(
      startDs: String,
      endDs: String,
      tablePartitionsDataset: String,
      tableStatsDataset: Option[String]
  ): Int = {
    Try {
      val metadata = node.metaData
      val range = PartitionRange(startDs, endDs)(PartitionSpec.daily)
      val kvStore = api.genKvStore

      logger.info(s"Starting batch node runner for '${metadata.name}'")
      val inputTablesToRange = Option(metadata.executionInfo.getTableDependencies)
        .map(_.asScala.toArray)
        .getOrElse(Array.empty)
        .filterNot(_.isSoftNodeDependency) // Skip table checks on soft dependencies
        .map((td) => {
          val inputPartSpec =
            Option(td.getTableInfo).map(_.partitionSpec(tableUtils.partitionSpec)).getOrElse(tableUtils.partitionSpec)
          td.getTableInfo.table -> DependencyResolver.computeInputRange(range, td).map(_.translate(inputPartSpec))
        })
        .toMap
      val allInputTablePartitions = inputTablesToRange.map {
        case (tableName, maybePartitionRange) => {
          // The partitions returned here are going to follow the tableUtils.partitionSpec default spec.
          tableName -> tableUtils.partitions(tableName, tablePartitionSpec = maybePartitionRange.map(_.partitionSpec))
        }
      }

      val maybeMissingPartitions = inputTablesToRange.map {
        case (tableName, maybePartitionRange) => {
          tableName -> maybePartitionRange.map((requestedPR) => {
            // Need to normalize back again to the default spec before diffing against the existing partitions.
            try {
              requestedPR.translate(tableUtils.partitionSpec).partitions.diff(allInputTablePartitions(tableName))
            } catch {
              case e: Exception =>
                logger.error(s"Error computing missing partitions for table $tableName.")
                throw e
            }
          })
        }
      }
      val kvStoreUpdates = kvStore.multiPut(allInputTablePartitions.map { case (tableName, allPartitions) =>
        val partitionsJson = PartitionRange.collapsedPrint(allPartitions)(range.partitionSpec)
        PutRequest(tableName.getBytes, partitionsJson.getBytes, tablePartitionsDataset)
      }.toSeq)

      Await.result(kvStoreUpdates, Duration.Inf)

      val missingPartitions = maybeMissingPartitions.collect {
        case (tableName, Some(missing)) if missing.nonEmpty =>
          tableName -> missing
      }
      if (missingPartitions.nonEmpty) {
        throw new RuntimeException(
          "The following input tables are missing partitions for the requested range:\n" +
            missingPartitions
              .map { case (tableName, missing) =>
                s"Table: $tableName, Missing Partitions: ${missing.mkString(", ")}"
              }
              .mkString("\n")
        )
      } else {
        run(metadata, node.content, Option(range))
        try {
          postJobActions(metadata = metadata,
                         range = range,
                         tablePartitionsDataset = tablePartitionsDataset,
                         kvStore = kvStore,
                         tableStatsDataset = tableStatsDataset)
        } catch {
          case e: Exception =>
            // Don't fail the job if post-job actions fail
            logger.error(s"Post-job actions failed for '${metadata.name}'", e)
        }
      }
    } match {
      case Success(_) => {
        logger.info("Batch node runner completed successfully")
        0
      }
      case Failure(e) => {
        logger.error(s"Batch node runner failed for '${node.metaData.name}'", e)
        1
      }
    }
  }
}

object BatchNodeRunner {

  def main(args: Array[String]): Unit = {
    val batchArgs = new BatchNodeRunnerArgs(args)
    val node = ThriftJsonCodec.fromJsonFile[Node](batchArgs.confPath(), check = false)
    val tableUtils = TableUtils(SparkSessionBuilder.build(s"batch-node-runner-${node.metaData.name}"))
    val api = instantiateApi(batchArgs.onlineClass(), batchArgs.apiProps)
    val runner = new BatchNodeRunner(node, tableUtils, api)
    val exitCode =
      runner.runFromArgs(batchArgs.startDs(),
                         batchArgs.endDs(),
                         batchArgs.tablePartitionsDataset(),
                         batchArgs.tableStatsDataset.toOption)
    tableUtils.sparkSession.stop()
    System.exit(exitCode)
  }

  def instantiateApi(onlineClass: String, props: Map[String, String]): Api = {
    val cl = Thread.currentThread().getContextClassLoader
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(props)
    onlineImpl.asInstanceOf[Api]
  }
}
