package ai.chronon.online.test

import ai.chronon.api.{Builders => B, _}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.fetcher.ModelTransformsFetcher
import ai.chronon.online.{DeployModelRequest, ModelPlatform, ModelPlatformProvider, PredictRequest, PredictResponse, TrainingRequest}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ModelTransformsFetcherTest extends AnyFlatSpec with Matchers with ScalaFutures {
  
  implicit val executionContext: ExecutionContext = ExecutionContext.global
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = 5.seconds)

  val testPlatformProvider: TestModelPlatformProvider = new TestModelPlatformProvider()
  val fetcher: ModelTransformsFetcher = new ModelTransformsFetcher(testPlatformProvider)

  "fetchModelTransforms" should "handle single model with no mappings" in {
    // Set up models, requests and mock platform
    val testModel = B.Model(
      metaData = B.MetaData(name = "test_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    val modelTransforms = B.ModelTransforms(
      metaData = B.MetaData(name = "test_model_transforms"),
      models = Seq(testModel)
    )

    val mockPlatform = new TestModelPlatform(Map(
      Map("user_id" -> "user123") -> Map("score" -> 0.85.asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val requests = Seq(
      Request("test_transform", Map("user_id" -> "user123"), None, None)
    )

    // trigger fetch
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify responses
    whenReady(responseFuture) { responses =>
      responses should have size 1

      val response = responses.head
      response.request.name shouldBe "test_transform"
      response.values shouldBe Success(Map("test_model__score" -> 0.85))
    }
  }

  "fetchModelTransforms" should "handle multiple requests with deduping" in {
    // Set up models, requests and mock platform
    val testModel = B.Model(
      metaData = B.MetaData(name = "scorer"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(testModel),
      metaData = B.MetaData(name = "test_model_transforms"),
    )

    val callTrackingPlatform = new CallTrackingModelPlatform(Map(
      Map("user_id" -> "user1") -> Map("score" -> 0.9.asInstanceOf[AnyRef]),
      Map("user_id" -> "user2") -> Map("score" -> 0.7.asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, callTrackingPlatform)

    val requests = Seq(
      Request("test1", Map("user_id" -> "user1"), None, None),
      Request("test2", Map("user_id" -> "user2"), None, None),
      Request("test3", Map("user_id" -> "user1"), None, None) // Duplicate input
    )

    // Trigger fetch
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify
    whenReady(responseFuture) { responses =>
      responses should have size 3
      
      // Check that deduping occurred
      callTrackingPlatform.predictCallCount shouldBe 1
      callTrackingPlatform.uniqueInputsProcessed shouldBe 2
      
      // Check responses
      responses(0).values shouldBe Success(Map("scorer__score" -> 0.9))
      responses(1).values shouldBe Success(Map("scorer__score" -> 0.7))
      responses(2).values shouldBe Success(Map("scorer__score" -> 0.9))
    }
  }

  "fetchModelTransforms" should "handle passthrough fields" in {
    // Set up models, requests and mock platform
    val testModel = B.Model(
      metaData = B.MetaData(name = "model1"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(testModel),
      metaData = B.MetaData(name = "test_model_transforms"),
      passthroughFields = Seq("user_id", "session_id")
    )

    val mockPlatform = new TestModelPlatform(Map(
      Map("user_id" -> "user123", "session_id" -> "sess456", "extra" -> "ignored") -> 
        Map("prediction" -> "positive".asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val requests = Seq(
      Request("test", Map("user_id" -> "user123", "session_id" -> "sess456", "extra" -> "ignored"), None, None)
    )

    // Trigger fetch
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify responses
    whenReady(responseFuture) { responses =>
      responses should have size 1
      
      val expectedResult = Map(
        "model1__prediction" -> "positive",
        "user_id" -> "user123",
        "session_id" -> "sess456"
        // "extra" should not be included as it's not in passthroughFields
      )

      responses.head.values shouldBe Success(expectedResult)
    }
  }

  "fetchModelTransforms" should "handle multiple models with result merging" in {
    // Set up models, requests and mock platform
    val embeddingModel = B.Model(
      metaData = B.MetaData(name = "embedding_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )
    
    val scoringModel = B.Model(
      metaData = B.MetaData(name = "scoring_model"), 
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )
    
    val classificationModel = B.Model(
      metaData = B.MetaData(name = "classification_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(embeddingModel, scoringModel, classificationModel),
      metaData = B.MetaData(name = "test_model_transforms"),
    )

    val multiOutputPlatform = new MultiOutputModelPlatform()
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, multiOutputPlatform)

    val requests = Seq(
      Request("test1", Map("user_id" -> "user1"), None, None),
      Request("test2", Map("user_id" -> "user2"), None, None)
    )

    // Trigger fetch
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify responses
    whenReady(responseFuture) { responses =>
      responses should have size 2
      
      // Check first response, confirm that it contains outputs from all 3 models with distinct field names
      val response1 = responses.head
      response1.request.name shouldBe "test1"
      response1.values should be a 'success
      val values1 = response1.values.get

      values1 should contain key "embedding_model__embeddings"
      values1 should contain key "scoring_model__score"
      values1 should contain key "classification_model__category"

      values1("embedding_model__embeddings") shouldBe Map("values" -> Seq(0.1, 0.2, 0.3))
      values1("scoring_model__score") shouldBe 0.85
      values1("classification_model__category") shouldBe "premium"

      // Check second response
      val response2 = responses(1)
      response2.request.name shouldBe "test2"
      response2.values should be a 'success
      val values2 = response2.values.get

      values2("embedding_model__embeddings") shouldBe Map("values" -> Seq(0.4, 0.5, 0.6))
      values2("scoring_model__score") shouldBe 0.72
      values2("classification_model__category") shouldBe "standard"
    }
  }

  "fetchModelTransforms" should "handle input and output mappings" in {
    // Set up models, schema, requests and mock platform
    val inputSchema = B.structSchema("input_schema",
      "user_id" -> StringType,
      "session_id" -> StringType
    )
    
    val outputSchema = B.structSchema("output_schema",
      "model_prediction" -> StringType,
      "model_confidence" -> DoubleType
    )
    
    val testModel = B.Model(
      metaData = B.MetaData(name = "mapping_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI),
      inputMapping = Map(
        "model_user_id" -> "user_id",        // model expects "model_user_id", we have "user_id"
        "model_session" -> "session_id"      // model expects "model_session", we have "session_id"
      ),
      outputMapping = Map(
        "prediction" -> "mapping_model__model_prediction",  // model returns "model_prediction" (prefixed as "mapping_model__model_prediction"), we want "prediction"
        "confidence" -> "mapping_model__model_confidence"   // model returns "model_confidence" (prefixed as "mapping_model__model_confidence"), we want "confidence"
      ),
      valueSchema = outputSchema
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(testModel),
      metaData = B.MetaData(name = "test_model_transforms"),
      keySchema = inputSchema
    )

    // Setup mock platform that expects mapped input field names
    val mockPlatform = new TestModelPlatform(Map(
      Map("model_user_id" -> "user123", "model_session" -> "sess456") -> Map(
        "model_prediction" -> "positive".asInstanceOf[AnyRef],
        "model_confidence" -> 0.95.asInstanceOf[AnyRef]
      )
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val requests = Seq(
      Request("test", Map("user_id" -> "user123", "session_id" -> "sess456"), None, None)
    )

    // Execute
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify
    whenReady(responseFuture) { responses =>
      responses should have size 1
      
      val response = responses.head
      response.request.name shouldBe "test"
      response.values should be a 'success
      
      val expectedResult = Map(
        "mapping_model__prediction" -> "positive",    // output mapping applied: model_prediction -> prediction, then prefixed
        "mapping_model__confidence" -> 0.95           // output mapping applied: model_confidence -> confidence, then prefixed
      )

      response.values.get shouldBe expectedResult
    }
  }

  "fetchModelTransforms" should "isolate model failures without affecting other models" in {
    // Setup 3 models: one that works, one that fails, and one that works
    val workingModel1 = B.Model(
      metaData = B.MetaData(name = "working_model_1"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )
    
    val failingModel = B.Model(
      metaData = B.MetaData(name = "failing_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.SageMaker)
    )
    
    val workingModel2 = B.Model(
      metaData = B.MetaData(name = "working_model_2"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(workingModel1, failingModel, workingModel2),
      metaData = B.MetaData(name = "test_model_transforms"),
    )

    val workingPlatform = new TestModelPlatform(Map(
      Map("user_id" -> "user123") -> Map("working_result" -> "success".asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, workingPlatform)
    
    // Setup failing platform
    val failingPlatform = new TestModelPlatform() {
      override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
        Future.failed(new RuntimeException("Model inference failed"))
      }
    }
    testPlatformProvider.addPlatform(ModelBackend.SageMaker, failingPlatform)

    val requests = Seq(
      Request("test", Map("user_id" -> "user123"), None, None)
    )

    // trigger fetch
    val responseFuture = fetcher.fetchModelTransforms(requests, modelTransforms)

    // Verify
    whenReady(responseFuture) { responses =>
      responses should have size 1
      
      val response = responses.head
      response.request.name shouldBe "test"
      response.values should be a 'success
      
      val values = response.values.get

      // Working models should have their results (prefixed with model name)
      values should contain key "working_model_1__working_result"
      values("working_model_1__working_result") shouldBe "success"
      values should contain key "working_model_2__working_result"
      values("working_model_2__working_result") shouldBe "success"

      // Failing model should have error feature
      values should contain key "failing_model_exception"
      values("failing_model_exception").toString should include("Model inference failed")
    }
  }

  "fetchJoinSourceModelTransforms" should "use join features correctly when join succeeds" in {
    val testModel = B.Model(
      metaData = B.MetaData(name = "join_only_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(testModel),
      metaData = B.MetaData(name = "test_model_transforms")
    )

    val mockPlatform = new TestModelPlatform(Map(
      Map("user_id" -> "user123", "join_feature_1" -> "value1", "join_feature_2" -> 42.asInstanceOf[AnyRef]) -> 
        Map("prediction" -> "success".asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val originalRequests = Seq(
      Request("test", Map("user_id" -> "user123"), None, None)
    )

    // Create simulated successful join response with join features
    val joinResponses = Seq(
      Response(originalRequests.head, Success(Map("join_feature_1" -> "value1", "join_feature_2" -> 42.asInstanceOf[AnyRef])))
    )

    // Trigger fetch join source model transforms
    val responseFuture = fetcher.fetchJoinSourceModelTransforms(originalRequests, modelTransforms, joinResponses)

    // Verify
    whenReady(responseFuture) { responses =>
      responses should have size 1
      
      val response = responses.head
      response.request.name shouldBe "test"
      response.values should be a 'success
      
      val values = response.values.get
      values should contain key "join_only_model__prediction"
      values("join_only_model__prediction") shouldBe "success"

      // Verify that the request keys only include original keys
      response.request.keys shouldBe Map("user_id" -> "user123")
    }
  }

  "fetchJoinSourceModelTransforms" should "handle mixed join success/failure scenarios with proper ordering" in {
    val testModel = B.Model(
      metaData = B.MetaData(name = "mixed_test_model"),
      inferenceSpec = B.InferenceSpec(ModelBackend.VertexAI)
    )

    val modelTransforms = B.ModelTransforms(
      models = Seq(testModel),
      metaData = B.MetaData(name = "test_model_transforms")
    )

    val mockPlatform = new TestModelPlatform(Map(
      Map("original_key" -> "orig1", "join_feature_1" -> "value1") -> Map("score" -> 0.8.asInstanceOf[AnyRef]),
      Map("original_key" -> "orig3", "join_feature_2" -> "value2") -> Map("score" -> 0.9.asInstanceOf[AnyRef]),
      Map("original_key" -> "orig5", "join_feature_4" -> "value4") -> Map("score" -> 0.7.asInstanceOf[AnyRef])
    ))
    testPlatformProvider.addPlatform(ModelBackend.VertexAI, mockPlatform)

    val originalRequests = Seq(
      Request("req1", Map("original_key" -> "orig1"), None, None),
      Request("req2", Map("original_key" -> "orig2"), None, None),
      Request("req3", Map("original_key" -> "orig3"), None, None),
      Request("req4", Map("original_key" -> "orig4"), None, None),
      Request("req5", Map("original_key" -> "orig5"), None, None)
    )

    // Create simulated join responses: positions 1 and 3 (0-indexed) fail, others succeed
    val joinResponses = Seq(
      Response(originalRequests(0), Success(Map("join_feature_1" -> "value1"))), // Success
      Response(originalRequests(1), Failure(new RuntimeException("Join failed for req2"))), // Failure
      Response(originalRequests(2), Success(Map("join_feature_2" -> "value2"))), // Success  
      Response(originalRequests(3), Failure(new RuntimeException("Join failed for req4"))), // Failure
      Response(originalRequests(4), Success(Map("join_feature_4" -> "value4")))  // Success
    )

    // Execute fetchJoinSourceModelTransforms directly
    val responseFuture = fetcher.fetchJoinSourceModelTransforms(originalRequests, modelTransforms, joinResponses)

    // Verify results
    whenReady(responseFuture) { responses =>
      responses should have size 5
      
      // Check order is preserved - response names should match original request names
      responses(0).request.name shouldBe "req1"
      responses(1).request.name shouldBe "req2"  
      responses(2).request.name shouldBe "req3"
      responses(3).request.name shouldBe "req4"
      responses(4).request.name shouldBe "req5"
      
      // Check successful join + model transform responses (positions 0, 2, 4)
      responses(0).values should be a 'success
      responses(0).values.get shouldBe Map("mixed_test_model__score" -> 0.8)
      responses(0).request.keys shouldBe Map("original_key" -> "orig1")

      responses(2).values should be a 'success
      responses(2).values.get shouldBe Map("mixed_test_model__score" -> 0.9)
      responses(2).request.keys shouldBe Map("original_key" -> "orig3")

      responses(4).values should be a 'success
      responses(4).values.get shouldBe Map("mixed_test_model__score" -> 0.7)
      responses(4).request.keys shouldBe Map("original_key" -> "orig5")
      
      // Check failed join responses (positions 1, 3) - should propagate join failures and preserve original request
      responses(1).values should be a 'failure
      responses(1).values.failed.get.getMessage should include("Join failed for req2")
      responses(1).request.keys shouldBe Map("original_key" -> "orig2") // Should preserve original keys
      
      responses(3).values should be a 'failure
      responses(3).values.failed.get.getMessage should include("Join failed for req4")
      responses(3).request.keys shouldBe Map("original_key" -> "orig4") // Should preserve original keys
    }
  }

  "fetchModelTransforms" should "fail fast when ModelPlatformProvider is null" in {
    assertThrows[IllegalArgumentException] {
      new ModelTransformsFetcher(null)
    }
  }
}

// Test stub implementations
class TestModelPlatformProvider extends ModelPlatformProvider {
  private val platforms = mutable.Map[ModelBackend, ModelPlatform]()
  
  def addPlatform(backend: ModelBackend, platform: ModelPlatform): Unit = {
    platforms(backend) = platform
  }
  
  override def getPlatform(modelBackend: ModelBackend, backendParams: Map[String, String]): ModelPlatform = {
    platforms.getOrElse(modelBackend, new TestModelPlatform())
  }
}

class TestModelPlatform(responseMap: Map[Map[String, AnyRef], Map[String, AnyRef]] = Map.empty) extends ModelPlatform {
  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    val outputs = predictRequest.inputRequests.map { inputRequest =>
      responseMap.get(inputRequest) match {
        case Some(outputs) => outputs
        case None => Map("default_output" -> "test_result".asInstanceOf[AnyRef])
      }
    }
    Future.successful(PredictResponse(predictRequest, Success(outputs)))
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ai.chronon.online.ModelOperation, id: String): Future[ai.chronon.online.ModelJobStatus] = ???
}

class CallTrackingModelPlatform(responseMap: Map[Map[String, AnyRef], Map[String, AnyRef]]) extends ModelPlatform {
  @volatile var predictCallCount: Int = 0
  @volatile var uniqueInputsProcessed: Int = 0
  
  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    predictCallCount += 1
    uniqueInputsProcessed = predictRequest.inputRequests.distinct.size
    
    val outputs = predictRequest.inputRequests.map { inputRequest =>
      responseMap.get(inputRequest) match {
        case Some(outputs) => outputs
        case None => Map("default_output" -> "test_result".asInstanceOf[AnyRef])
      }
    }
    Future.successful(PredictResponse(predictRequest, Success(outputs)))
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ai.chronon.online.ModelOperation, id: String): Future[ai.chronon.online.ModelJobStatus] = ???
}

// Platform that returns different outputs based on the model name in the request
class MultiOutputModelPlatform extends ModelPlatform {
  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    val outputs = predictRequest.inputRequests.map { inputRequest =>
      val modelName = predictRequest.model.metaData.name
      val userId = inputRequest.get("user_id").map(_.toString).getOrElse("unknown")
      
      val predictionData = (modelName, userId) match {
        case ("embedding_model", "user1") => Map("embeddings" -> Map("values" -> Seq(0.1, 0.2, 0.3).asInstanceOf[AnyRef]).asInstanceOf[AnyRef])
        case ("embedding_model", "user2") => Map("embeddings" -> Map("values" -> Seq(0.4, 0.5, 0.6).asInstanceOf[AnyRef]).asInstanceOf[AnyRef])
        case ("scoring_model", "user1") => Map("score" -> 0.85.asInstanceOf[AnyRef])
        case ("scoring_model", "user2") => Map("score" -> 0.72.asInstanceOf[AnyRef])
        case ("classification_model", "user1") => Map("category" -> "premium".asInstanceOf[AnyRef])
        case ("classification_model", "user2") => Map("category" -> "standard".asInstanceOf[AnyRef])
        case _ => Map("default" -> "unknown".asInstanceOf[AnyRef])
      }
      
      predictionData
    }
    
    Future.successful(PredictResponse(predictRequest, Success(outputs)))
  }

  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ai.chronon.online.ModelOperation, id: String): Future[ai.chronon.online.ModelJobStatus] = ???
}