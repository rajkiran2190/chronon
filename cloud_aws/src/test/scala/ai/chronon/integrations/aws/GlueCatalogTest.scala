package ai.chronon.integrations.aws

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class GlueCatalogTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    classOf[GlueCatalogTest].getSimpleName,
    local = true,
    additionalConfig = Some(
      Map(
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  "basic round trip hive table" should "work with local metastore" in {
    import spark.implicits._

    val input = Set(1, 2, 3, 4)
    val sourceDF = spark.sparkContext.parallelize(input.toSeq).toDF("id")

    sourceDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test_hive_table")

    val back = spark.table("test_hive_table").select("id").as[Int].collect()
    assertEquals(input, back.toSet)

  }
}
