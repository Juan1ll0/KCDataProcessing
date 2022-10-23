package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Practica Juan Ignacio fernandez Diez Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  override def computeTotalBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"antenna_id")
      .groupBy($"antenna_id", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("bytes_by_antenna")
      )
      .select(
        $"window.start".as("timestamp"),
        $"antenna_id".as("id"),
        $"bytes_by_antenna".as("value"),
        lit("antenna_total_bytes") as ("type")
      )
  }

  override def computeTotalBytesByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"email")
      .groupBy($"email", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("bytes_by_user")
      )
      .select(
        $"window.start".as("timestamp"),
        $"email".as("id"),
        $"bytes_by_user".as("value"),
        lit("user_total_bytes") as ("type")
      )
  }


  override def computeTotalBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"app")
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("bytes_by_app")
      )
      .select(
        $"window.start".as("timestamp"),
        $"app".as("id"),
        $"bytes_by_app".as("value"),
        lit("app_total_bytes") as ("type")
      )
  }

  override def computeUsersOverQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"id", $"email", $"quota")
      .groupBy($"id", $"email", $"quota", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("bytes_by_user")
      )
      .where("bytes_by_user > quota")
      .select(
        $"email".as("email"),
        $"bytes_by_user".as("usage"),
        $"quota".as("quota"),
        $"window.start".as("timestamp")
      )
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  /**
   * Main for Batch job
   *
   * @param args arguments for execution:
   *             filterDate storagePath jdbcUri jdbcMetadataTable aggJdbcTable aggJdbcErrorTable aggJdbcPercentTable jdbcUser jdbcPassword
   * Example:
   *   2007-01-23T10:15:30Z /tmp/batch-storage jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_1h_agg antenna_errors_agg antenna_percent_agg postgres keepcoding
   */
  // puedes descomentar este main para pasar parametros a la ejecucion
  //def main(args: Array[String]): Unit = run(args)

  def main(args: Array[String]): Unit = {
    /* Configure IP's */
    val IPPostgres = "104.155.117.122"

    val jdbcUri = s"jdbc:postgresql://$IPPostgres:5432/postgres"
    val jdbcUser = "postgres"
    val jdbcPassword = "postgres"

    val offsetDateTime = OffsetDateTime.parse("2022-10-23T10:00:00Z")
    val parquetDF = readFromStorage("/tmp/proyecto/antenna_parquet", offsetDateTime)
    val metadataDF = readUserMetadata(jdbcUri, "user_metadata", jdbcUser, jdbcPassword)
    val enrichDF = enrichAntennaWithMetadata(parquetDF, metadataDF).cache()

    val aggByAntenna = computeTotalBytesByAntenna(enrichDF)
    val aggByUser = computeTotalBytesByUser(enrichDF)
    val aggByApp = computeTotalBytesByApp(enrichDF)
    val aggOverQuota = computeUsersOverQuota(enrichDF)
    //
    writeToJdbc(aggByAntenna, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggByUser, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggByApp, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggOverQuota, jdbcUri, "user_quota_limit", jdbcUser, jdbcPassword)
    writeToStorage(parquetDF, "/tmp/proyecto/antenna_parquet/")

    spark.close()
  }

}
