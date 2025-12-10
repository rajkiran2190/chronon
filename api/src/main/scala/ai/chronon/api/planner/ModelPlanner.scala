package ai.chronon.api.planner

import ai.chronon.api.{Model, PartitionSpec, TableDependency, TableInfo}
import ai.chronon.api.Extensions.{MetadataOps, WindowUtils}
import ai.chronon.planner.{ConfPlan, CreateModelEndpointNode, DeployModelNode, Node}
import ai.chronon.planner

import scala.collection.JavaConverters._

class ModelPlanner(model: Model)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[Model](model)(outputPartitionSpec) {

  private def eraseExecutionInfo: Model = {
    val result = model.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticModel(model: Model): Model = {
    val semantic = model.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  def createEndpointNode: Node = {
    // todo - add dependency on training step once we add the node
    val metaData =
      MetaDataUtils.layer(
        model.metaData,
        "model_create_endpoint",
        model.metaData.name + "__model_create_endpoint",
        Seq.empty,
        None
      )

    val node = new CreateModelEndpointNode().setModel(eraseExecutionInfo)

    val copy = semanticModel(model)

    toNode(metaData, _.setCreateModelEndpoint(node), copy)
  }

  def deployModelNode: Node = {
    val stepDays = 1 // Default step days for model deployment

    // Deploy depends on the endpoint being created
    val createEndpoint = createEndpointNode
    val tableDep = new TableDependency()
      .setTableInfo(
        new TableInfo()
          .setTable(createEndpoint.metaData.outputTable)
      )
      .setStartOffset(WindowUtils.zero())
      .setEndOffset(WindowUtils.zero())
    val tableDeps = Seq(tableDep)

    val metaData =
      MetaDataUtils.layer(
        model.metaData,
        "model_deploy",
        model.metaData.name + "__model_deploy",
        tableDeps,
        Some(stepDays)
      )

    val node = new DeployModelNode().setModel(eraseExecutionInfo)

    val copy = semanticModel(model)

    toNode(metaData, _.setDeployModel(node), copy)
  }

  override def buildPlan: ConfPlan = {
    val createEndpoint = createEndpointNode
    val deploy = deployModelNode

    val terminalNodeNames = Map(
      planner.Mode.DEPLOY -> deploy.metaData.name
    )

    new ConfPlan()
      .setNodes(Seq(createEndpoint, deploy).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object ModelPlanner {
  def apply(model: Model)(implicit outputPartitionSpec: PartitionSpec): ModelPlanner =
    new ModelPlanner(model)(outputPartitionSpec)
}
