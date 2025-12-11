package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.EndpointConfig
import ai.chronon.online.{
  DeployModel,
  DeployModelRequest,
  ModelJobStatus,
  ModelOperation,
  ModelPlatform,
  PredictRequest,
  PredictResponse,
  SubmitTrainingJob,
  TrainingRequest
}
import io.vertx.core.Vertx
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._

class VertexPlatform(project: String, location: String, webClient: Option[WebClient] = None) extends ModelPlatform {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val client = webClient.getOrElse {
    val vertx = Vertx.vertx()
    WebClient.create(vertx)
  }

  private lazy val httpClient = new VertexHttpClient(client)

  // delegate orchestration tasks to VertexOrchestration
  private lazy val orchestration = new VertexOrchestration(project, location)

  val urlTemplates = Map(
    "publisher" -> s"https://$location-aiplatform.googleapis.com/v1/projects/$project/locations/$location/publishers/google/models/%s:predict",
    "custom" -> s"https://$location-aiplatform.googleapis.com/v1/projects/$project/locations/$location/endpoints/%s:predict"
  )

  private val endpointNameToIdCache = new ConcurrentHashMap[String, String]()

  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    val promise = Promise[PredictResponse]()

    try {
      val modelParams =
        Option(predictRequest.model.inferenceSpec.modelBackendParams)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)
      val modelType = modelParams.getOrElse("model_type", "publisher")

      // Handle missing model name by returning a Failure
      val modelName = modelParams.get("model_name") match {
        case Some(name) => name
        case None =>
          promise.success(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException("model_name is required in modelBackendParams"))))
          return promise.future
      }

      // Validate model type and get URL template
      val urlTemplate = urlTemplates.get(modelType) match {
        case Some(template) => template
        case None =>
          promise.success(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException(s"Unsupported model_type: $modelType"))))
          return promise.future
      }

      val url = if (modelType == "custom") {
        val endpointIdOrNull = lookupCustomEndpointId(modelName)
        if (endpointIdOrNull == null) {
          promise.success(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException(s"Endpoint not found for model: $modelName"))))
          return promise.future
        }

        urlTemplate.format(endpointIdOrNull)
      } else
        urlTemplate.format(modelName)

      // Validate all inputs have 'instance' key
      val missingInstanceIndices = predictRequest.inputRequests.zipWithIndex.collect {
        case (inputRequest, index) if !inputRequest.contains("instance") => index
      }

      if (missingInstanceIndices.nonEmpty) {
        val errorMsg = s"Missing 'instance' key in input requests at indices: ${missingInstanceIndices.mkString(", ")}"
        promise.success(PredictResponse(predictRequest, Failure(new IllegalArgumentException(errorMsg))))
        return promise.future
      }

      val requestBody = VertexHttpUtils.createPredictionRequestBody(predictRequest.inputRequests, modelParams)

      httpClient.makeHttpRequest(url, PostMethod, Some(requestBody)) { response =>
        if (response != null) {
          if (response.statusCode() == 200) {
            val responseBody = response.bodyAsJsonObject()
            val results = VertexHttpUtils.extractPredictionResults(responseBody)
            promise.success(PredictResponse(predictRequest, Success(results)))
          } else {
            val errorMsg = s"HTTP Request failed: ${response.statusCode()}: ${response.bodyAsString()}"
            promise.success(PredictResponse(predictRequest, Failure(new RuntimeException(errorMsg))))
          }
        } else {
          promise.success(PredictResponse(predictRequest, Failure(new RuntimeException("HTTP Request failed"))))
        }
      }
    } catch {
      case e: Exception =>
        promise.success(PredictResponse(predictRequest, Failure(e)))
    }

    promise.future
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = {
    orchestration.submitTrainingJob(trainingRequest)
  }

  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = {
    orchestration.createEndpoint(endpointConfig.getEndpointName)
  }

  override def getJobStatus(operation: ModelOperation, id: String): Future[ModelJobStatus] = {
    operation match {
      case SubmitTrainingJob => orchestration.getTrainingJobStatus(id)
      case DeployModel       => orchestration.getOperationStatus(id)
    }
  }

  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = {
    orchestration.deployModel(deployModelRequest)
  }

  private def lookupCustomEndpointId(modelName: String): String = {
    val cached = endpointNameToIdCache.get(modelName)
    if (cached != null) return cached

    val maybeEndpointName = orchestration.findEndpointByName(modelName)
    maybeEndpointName match {
      case Some(name) =>
        val endpointId = name.split("/").last
        endpointNameToIdCache.putIfAbsent(modelName, endpointId)
        endpointId
      case None => null
    }
  }
}
