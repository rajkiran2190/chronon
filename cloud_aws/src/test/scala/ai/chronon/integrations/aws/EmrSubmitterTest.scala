package ai.chronon.integrations.aws

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.spark.submission.JobSubmitterConstants._
import ai.chronon.spark.submission.SparkJob
import org.junit.Assert.assertEquals
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.emr.EmrClient
import software.amazon.awssdk.services.emr.model.{AddJobFlowStepsRequest, AddJobFlowStepsResponse}

class EmrSubmitterTest extends AnyFlatSpec with MockitoSugar {
  "EmrSubmitterClient" should "return job id when a job is submitted and assert EMR request args" in {
    val stepId = "mock-step-id"
    val clusterId = "j-MOCKCLUSTERID123"

    val mockEmrClient = mock[EmrClient]
    val mockEc2Client = mock[Ec2Client]

    val requestCaptor = org.mockito.ArgumentCaptor.forClass(classOf[AddJobFlowStepsRequest])

    when(
      mockEmrClient.addJobFlowSteps(
        requestCaptor.capture()
      )).thenReturn(AddJobFlowStepsResponse.builder().stepIds(stepId).build())

    val expectedCustomerId = "canary"
    val expectedApplicationArgs = Seq("group-by-backfill", "arg1", "arg2")
    val expectedFiles = List("s3://random-conf", "s3://random-data")
    val expectedMainClass = "some-main-class"
    val expectedJarURI = "s3://-random-jar-uri"

    val submitter = new EmrSubmitter(expectedCustomerId, mockEmrClient, mockEc2Client)
    val submittedStepId = submitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> expectedMainClass,
        JarURI -> expectedJarURI,
        ClusterId -> clusterId
      ),
      jobProperties = Map.empty,
      files = expectedFiles,
      labels = Map.empty,
      expectedApplicationArgs: _*
    )
    assertEquals(submittedStepId, stepId)

    val actualRequest = requestCaptor.getValue

    // Verify the cluster ID is correct
    assertEquals(actualRequest.jobFlowId(), clusterId)

    // Verify step configuration
    assertEquals(actualRequest.steps().size(), 1)

    val stepConfig = actualRequest.steps().get(0)
    assertEquals(stepConfig.actionOnFailure().name(), "CANCEL_AND_WAIT")
    assertEquals(stepConfig.name(), "Run Zipline Job")
    assertEquals(stepConfig.hadoopJarStep().jar(), "command-runner.jar")
    assertEquals(
      stepConfig.hadoopJarStep().args().toScala.mkString(" "),
      s"bash -c aws s3 cp s3://random-conf /mnt/zipline/; \naws s3 cp s3://random-data /mnt/zipline/; \nspark-submit --class some-main-class s3://-random-jar-uri group-by-backfill arg1 arg2"
    )
  }

  it should "test flink job locally" ignore {}

  it should "test flink kafka ingest job locally" ignore {}

  it should "Used to iterate locally. Do not enable this in CI/CD!" ignore {
    val emrSubmitter = new EmrSubmitter(
      "canary",
      EmrClient.builder().build(),
      Ec2Client.builder().build()
    )
    val jobId = emrSubmitter.submit(
      jobType = SparkJob,
      submissionProperties = Map(
        MainClass -> "ai.chronon.spark.Driver",
        JarURI -> "s3://zipline-artifacts-canary/jars/cloud_aws_lib_deploy.jar",
        ClusterId -> "j-13BASWFP15TLR"
      ),
      jobProperties = Map.empty,
      files = List("s3://zipline-warehouse-canary/purchases.v1"),
      Map.empty,
      "group-by-backfill",
      "--conf-path",
      "/mnt/zipline/purchases.v1",
      "--end-date",
      "2025-02-26",
      "--conf-type",
      "group_bys",
    )
    println("EMR job id: " + jobId)
    0
  }

}
