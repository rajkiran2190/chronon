package ai.chronon.api.test.planner

import ai.chronon.api.{Builders => B, _}
import ai.chronon.api.planner.ModelPlanner
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ModelPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def buildModel(name: String): Model = {
    B.Model(
      metaData = B.MetaData(
        name = name,
        namespace = "test_namespace"
      ),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project", "region" -> "us-central1")
      )
    )
  }

  "ModelPlanner" should "create both createEndpoint and deployModel nodes" in {
    val model = buildModel("test_model")
    val planner = new ModelPlanner(model)
    val plan = planner.buildPlan

    // Should create plan with both nodes
    plan.nodes.asScala should have size 2

    // Find both nodes
    val createEndpointNode = plan.nodes.asScala.find(_.content.isSetCreateModelEndpoint)
    val deployModelNode = plan.nodes.asScala.find(_.content.isSetDeployModel)

    createEndpointNode should be(defined)
    deployModelNode should be(defined)

    // Verify createEndpoint node
    createEndpointNode.get.content.getCreateModelEndpoint.model should not be null
    createEndpointNode.get.metaData.name should equal(s"${model.metaData.name}__model_create_endpoint")

    // Verify no table dependencies for endpoint creation
    val createEndpointDeps = createEndpointNode.get.metaData.executionInfo.tableDependencies.asScala
    createEndpointDeps should be(empty)

    // Verify deployModel node
    deployModelNode.get.content.getDeployModel.model should not be null
    deployModelNode.get.metaData.name should equal(s"${model.metaData.name}__model_deploy")

    // Verify table dependencies - should depend on createEndpoint
    val deployDeps = deployModelNode.get.metaData.executionInfo.tableDependencies.asScala
    deployDeps should have size 1
    deployDeps.head.tableInfo.table shouldBe createEndpointNode.get.metaData.executionInfo.outputTableInfo.table

    // Verify step days for deployment
    deployModelNode.get.metaData.executionInfo.isSetStepDays shouldBe true
    deployModelNode.get.metaData.executionInfo.stepDays shouldBe 1
  }

  it should "only create DEPLOY terminal node (not BACKFILL)" in {
    val model = buildModel("test_model")
    val planner = new ModelPlanner(model)
    val plan = planner.buildPlan

    // Verify terminal nodes - should only have DEPLOY
    plan.terminalNodeNames.asScala.size shouldBe 1
    plan.terminalNodeNames.containsKey(Mode.DEPLOY) shouldBe true
    plan.terminalNodeNames.containsKey(Mode.BACKFILL) shouldBe false

    // Verify DEPLOY points to deployModel node
    val deployModelNode = plan.nodes.asScala.find(_.content.isSetDeployModel)
    deployModelNode should be(defined)
    plan.terminalNodeNames.get(Mode.DEPLOY) shouldBe deployModelNode.get.metaData.name
  }

  it should "handle models with different backend types" in {
    val vertexModel = B.Model(
      metaData = B.MetaData(name = "vertex_model", namespace = "test_namespace"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("project" -> "test-project")
      )
    )

    val sageMakerModel = B.Model(
      metaData = B.MetaData(name = "sagemaker_model", namespace = "test_namespace"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.SageMaker,
        modelBackendParams = Map("region" -> "us-west-2")
      )
    )

    val vertexPlanner = new ModelPlanner(vertexModel)
    val sageMakerPlanner = new ModelPlanner(sageMakerModel)

    noException should be thrownBy {
      vertexPlanner.buildPlan
      sageMakerPlanner.buildPlan
    }

    // Verify both plans are created successfully with 2 nodes each
    vertexPlanner.buildPlan.nodes.asScala should have size 2
    sageMakerPlanner.buildPlan.nodes.asScala should have size 2
  }
}
