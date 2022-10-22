package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.Duration

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Practica Juan Ignacio fernandez Diez Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    )
    )

    dataFrame
      .select(from_json(col("value").cast(StringType), jsonSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
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
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "30 seconds"))
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
      .select($"timestamp", $"bytes", $"id")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "30 seconds"))
      .agg(
          sum($"bytes").as("bytes_by_user")
      )
      .select(
        $"window.start".as("timestamp"),
        $"id".as("id"),
        $"bytes_by_user".as("value"),
        lit("user_total_bytes") as ("type")
      )
  }

  override def computeTotalBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"app")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "30 seconds"))
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

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"timestamp", $"id", $"antenna_id", $"bytes", $"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()
  }

  /**
   * Main for Streaming job
   *
   * @param args arguments for execution:
   *             kafkaServer topic jdbcUri jdbcMetadataTable aggJdbcTable jdbcUser jdbcPassword storagePath
   * Example:
   *   XXX.XXX.XXX.XXX:9092 antenna_telemetry jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_agg postgres keepcoding /tmp/batch-storage
   */
  // Puedes descomentar este main y llamar con argumentos
  //def main(args: Array[String]): Unit = run(args)


  def main(args: Array[String]): Unit = {
    //run(args)
    /* Configure IP's */
    val ipPostgres = "104.155.117.122"
    val ipKafka = "34.88.129.157"
    val jdbcUser = "postgres"
    val jdbcPassword = "postgres"

    val kafkaDF = readFromKafka(s"$ipKafka:9092", "devices")
    val parsedDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(parsedDF, "/tmp/proyecto/antenna_parquet/")
    val metadaDF = readUserMetadata(s"jdbc:postgresql://$ipPostgres:5432/postgres", "user_metadata", jdbcUser, jdbcPassword)
    val enrichDF = enrichAntennaWithMetadata(parsedDF, metadaDF)
    val bytesByAntenna = computeTotalBytesByAntenna(enrichDF)
    val bytesByUser = computeTotalBytesByUser(enrichDF)
    val bytesByApp = computeTotalBytesByApp(enrichDF)

    val jdbcFutureAntenna = writeToJdbc(bytesByAntenna, s"jdbc:postgresql://$ipPostgres:5432/postgres", "bytes", jdbcUser, jdbcPassword)
    val jdbcFutureUser = writeToJdbc(bytesByUser, s"jdbc:postgresql://$ipPostgres:5432/postgres", "bytes", jdbcUser, jdbcPassword)
    val jdbcFutureApp = writeToJdbc(bytesByApp, s"jdbc:postgresql://$ipPostgres:5432/postgres", "bytes", jdbcUser, jdbcPassword)

//    bytesByAntenna
//      .writeStream
//      .format("console")
//      .start()
//      .awaitTermination()
     Await.result(
       Future.sequence(Seq(storageFuture, jdbcFutureAntenna, jdbcFutureUser, jdbcFutureApp)), Duration.Inf
     )

     spark.close()
  }
}
