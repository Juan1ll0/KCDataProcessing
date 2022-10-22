package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeTotalBytesByAntenna(dataFrame: DataFrame): DataFrame
  def computeTotalBytesByUser(dataFrame: DataFrame): DataFrame
  def computeTotalBytesByApp(dataFrame: DataFrame): DataFrame

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]
  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
    val aggBytesByAntenna = computeTotalBytesByAntenna(antennaMetadataDF)
    val aggBytesByUser = computeTotalBytesByUser(antennaMetadataDF)
    val aggBytesByApp = computeTotalBytesByApp(antennaMetadataDF)

    val jdbcFutureAntenna = writeToJdbc(aggBytesByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val jdbcFutureUser = writeToJdbc(aggBytesByUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val jdbcFutureApp = writeToJdbc(aggBytesByApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFutureAntenna, jdbcFutureUser, jdbcFutureApp)), Duration.Inf
    )

    spark.close()
  }

}
