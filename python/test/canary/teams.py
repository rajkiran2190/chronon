from gen_thrift.api.ttypes import Team

from ai.chronon.repo.cluster import generate_dataproc_cluster_config, generate_emr_cluster_config
from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ClusterConfigProperties, ConfigProperties, EnvironmentVariables

default = Team(
    description="Default team",
    email="ml-infra@<customer>.com",  # TODO: Infra team email
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.column": "ds",
        }
    ),
    env=EnvironmentVariables(
        common={
            "VERSION": "latest",
            "JOB_MODE": "local[*]",
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "CUSTOMER_ID": "dev",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
        },
    ),
)

test = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={
            "GCP_BIGTABLE_INSTANCE_ID": "test-instance"  # example, custom bigtable instance
        },
        modeEnvironments={
            RunMode.BACKFILL: {
            },
            RunMode.UPLOAD: {
            }
        }
    ),
)

gcp = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "gcp",
            "CUSTOMER_ID": "dev",
            "VERSION": "latest",
            "JOB_MODE": "local[*]",
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "GCP_PROJECT_ID": "canary-443022",
            "GCP_REGION": "us-central1",
            "GCP_DATAPROC_CLUSTER_NAME": "zipline-canary-cluster",
            "GCP_BIGTABLE_INSTANCE_ID": "zipline-canary-instance",
            "ENABLE_PUBSUB": "true",
            "ARTIFACT_PREFIX": "gs://zipline-artifacts-dev",
            "FLINK_STATE_URI": "gs://zipline-warehouse-canary/flink-state",
            "CHRONON_ONLINE_ARGS": " -Ztasks=4",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
                "GCP_DATAPROC_CLUSTER_NAME": "zipline-transient-upload-cluster"
            }
        }
    ),
    conf=ConfigProperties(
        common={
            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.table.gcs.temporary_gcs_bucket": "zipline-warehouse-canary",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table.gcs.connector_output_dataset": "data",
            "spark.chronon.table.gcs.connector_output_project": "canary-443022",
            "spark.chronon.table_write.prefix": "gs://zipline-warehouse-canary/data/tables/",
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.catalog.spark_catalog.warehouse": "gs://zipline-warehouse-canary/data/tables/",
            "spark.sql.catalog.spark_catalog.gcp.bigquery.location": "us-central1",
            "spark.sql.catalog.spark_catalog.gcp.bigquery.project-id": "canary-443022",
            "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.spark_catalog": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.spark_catalog.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.catalog.default_iceberg.warehouse": "gs://zipline-warehouse-canary/data/tables/",
            "spark.sql.catalog.default_iceberg.gcp.bigquery.location": "us-central1",
            "spark.sql.catalog.default_iceberg.gcp.bigquery.project-id": "canary-443022",
            "spark.sql.catalog.default_iceberg.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
            "spark.sql.catalog.default_iceberg": "ai.chronon.integrations.cloud_gcp.DelegatingBigQueryMetastoreCatalog",
            "spark.sql.catalog.default_iceberg.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.kryo.registrator": "ai.chronon.integrations.cloud_gcp.ChrononIcebergKryoRegistrator",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
        modeConfigs={
            RunMode.BACKFILL: {
                "spark.chronon.backfill_cloud_provider": "gcp",  # dummy test config
            }
        }
    ),
    clusterConf=ClusterConfigProperties(
        modeClusterConfigs={
            RunMode.UPLOAD: {
                "dataproc.config": generate_dataproc_cluster_config(2, "canary-443022", "gs://zipline-artifacts-canary",
                                                                    idle_timeout="300s",
                                                                    worker_host_type="n2-highmem-4",
                                                                    master_host_type="n2-highmem-8")
            }
        }
    ),
)

aws = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={
            "CLOUD_PROVIDER": "aws",
            "CUSTOMER_ID": "dev",
            "VERSION": "latest",
            "JOB_MODE": "local[*]",
            "PARTITION_COLUMN": "ds",
            "PARTITION_FORMAT": "yyyy-MM-dd",
            "AWS_REGION": "us-west-2",
            "EMR_CLUSTER_NAME": "zipline-canary-emr",
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-dev",
            "FLINK_STATE_URI": "s3://zipline-warehouse-dev/flink-state",
            "CHRONON_ONLINE_ARGS": " -Ztasks=4",
            "FRONTEND_URL": "http://localhost:3000",
            "HUB_URL": "http://localhost:3903",
        },
        modeEnvironments={
            RunMode.UPLOAD: {
            }
        }
    ),
    conf=ConfigProperties(
        common={
            "spark.chronon.partition.format": "yyyy-MM-dd",
            "spark.chronon.partition.column": "ds",
            "spark.chronon.table_write.prefix": "s3://zipline-warehouse-dev/data/tables/",
            "spark.chronon.table_write.format": "iceberg",
            "spark.sql.catalog.spark_catalog.warehouse": "s3://zipline-warehouse-dev/data/tables/",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.default_iceberg.warehouse": "s3://zipline-warehouse-dev/data/tables/",
            "spark.sql.catalog.default_iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.default_iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "false",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
        },
        modeConfigs={
            RunMode.BACKFILL: {
            }
        }
    ),
    clusterConf=ClusterConfigProperties(
        common={
            "emr.config": generate_emr_cluster_config(
                instance_count=3,
                subnet_name="zipline-canary-subnet-main",
                security_group_name="zipline-canary-sg",
                instance_type="m5.xlarge",
                idle_timeout=300,
                release_label="emr-7.12.0"
            )
        }
    ),
)
