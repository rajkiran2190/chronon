namespace py gen_thrift.planner
namespace java ai.chronon.planner

include "common.thrift"
include "api.thrift"

struct SourceWithFilterNode {
    2: optional api.Source source
    3: optional map<string,list<string>> excludeKeys
}

struct JoinBootstrapNode {
    2: optional api.Join join
}

struct JoinMergeNode {
    2: optional api.Join join
    // If set, the merge job will look for the output table of the production version to reuse as many unchanged columns as possible
    3: optional api.Join productionJoin
}

struct JoinDerivationNode {
    2: optional api.Join join
}

struct JoinPartNode {
    2: optional string leftSourceTable
    3: optional api.DataModel leftDataModel
    4: optional api.JoinPart joinPart
    5: optional map<string, list<string>> skewKeys
    // If set, the merge job will look for the output table of the production version to reuse as many unchanged columns as possible
    6: optional api.JoinPart productionJoinPart
}

struct MonolithJoinNode {
    1: optional api.Join join
}

struct UnionJoinNode {
    1: optional api.Join join
}

struct StagingQueryNode {
    2: optional api.StagingQuery stagingQuery
}

struct GroupByBackfillNode {
    2: optional api.GroupBy groupBy
}

struct GroupByUploadNode {
    2: optional api.GroupBy groupBy
}

struct GroupByUploadToKVNode {
    2: optional api.GroupBy groupBy
}

struct GroupByStreamingNode {
    2: optional api.GroupBy groupBy
}

struct JoinMetadataUpload {
    2: optional api.Join join
}

struct ModelTransformsUploadNode {
    2: optional api.ModelTransforms modelTransforms
}

struct ModelTransformsBackfillNode {
    2: optional api.ModelTransforms modelTransforms
}

struct CreateModelEndpointNode {
    2: optional api.Model model
}

struct DeployModelNode {
    2: optional api.Model model
}

struct ExternalSourceSensorNode {
    1: optional api.MetaData metaData
    2: optional common.TableDependency sourceTableDependency
    3: optional i64 retryCount
    4: optional i64 retryIntervalMin
    // When running TriggerExpr we need to use the proper engine.
    5: optional api.EngineType engineType
}
union NodeContent {
    // join nodes
    1: SourceWithFilterNode sourceWithFilter
    2: JoinBootstrapNode joinBootstrap
    3: JoinPartNode joinPart
    4: JoinMergeNode joinMerge
    5: JoinDerivationNode joinDerivation
    7: MonolithJoinNode monolithJoin
    8: StagingQueryNode stagingQuery
    9: UnionJoinNode unionJoin

    10: JoinMetadataUpload joinMetadataUpload
    11: ExternalSourceSensorNode externalSourceSensor
    12: ModelTransformsUploadNode modelTransformsUpload
    13: ModelTransformsBackfillNode modelTransformsBackfill
    14: CreateModelEndpointNode createModelEndpoint
    15: DeployModelNode deployModel

    // groupBy nodes
    100: GroupByBackfillNode groupByBackfill
    101: GroupByUploadNode groupByUpload
    102: GroupByStreamingNode groupByStreaming
    103: GroupByUploadToKVNode groupByUploadToKV

    // stagingQuery nodes
    200: api.StagingQuery stagingQueryBackfill

    // TODO: add metrics nodes
}

enum Mode {
    BACKFILL = 0,
    DEPLOY = 1,
    MONITOR = 2
}

struct Node {
    1: optional api.MetaData metaData
    2: optional NodeContent content
    3: optional string semanticHash
    4: optional bool isLongRunning
}

// some nodes could be depended on by both offline and online nodes
struct ConfPlan {
    1: optional list<Node> nodes
    2: optional map<Mode, string> terminalNodeNames
}

// just the information needed by the workflow engine
struct NodeMeta {
    1: string name
    2: string hash
    3: list<common.TableDependency> deps
    4: bool continuous
    5: optional i32 stepSize
    6: optional string outputTable
    7: optional i32 retries

}

struct WorkflowPlan {
    1: list<NodeMeta> nodes
    2: string terminalNode
}