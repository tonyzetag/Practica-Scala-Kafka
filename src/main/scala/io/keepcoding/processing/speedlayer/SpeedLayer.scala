package io.keepcoding.processing.speedlayer

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

// Defino la case class para los mensajes recibidos de la antenas relativo a los dispositivos conectados en json
// {"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "antenna_id": "550e8400-1234-1234-a716-446655440000", "app": "SKYPE", "bytes": 100}
case class AntennaMessage(timestamp: Timestamp, id: String, antenna_id: String, bytes: Long, app: String)

trait SpeedLayer {
  // Inicio SparkSession
  val spark: SparkSession

  // Leo desde Kafka
  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  // Parseo (Json a Dataframe)  | timestamp | id | antenna_id | bytes | app |
  def parserJsonData(dataFrame: DataFrame): DataFrame

  // Leo información de usuarios
  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  // Uno información de dispositivos con usuarios
  def enrichDeviceWithMetadata(devicesDF: DataFrame, usersDF: DataFrame): DataFrame

  // Métricas agregadas cada 5 minutos (Total de bytes {rx_antenna, tx_user, tx_app})
  def computeBytesTxRxUser(dataFrame: DataFrame): DataFrame
  def computeBytesTxRxApp(dataFrame: DataFrame): DataFrame
  def computeBytesTxRxAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesTxRx(userDF: DataFrame, appDF: DataFrame, antennaDF: DataFrame): DataFrame

  // Guarda en PortgreSQL Por cada Batch - (En paralelo con writeToStorage - Future)
  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  // Guarda en GC_Storage Por cada Batch - (En paralelo con writeToJdbc - Future)
  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  // Run para ejecutar
  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    println("kafkaDF...")
    val kafkaDF = readFromKafka(kafkaServer, topic)
    println("deviceDF...")
    val deviceDF = parserJsonData(kafkaDF)
    println("userDF...")
    val userDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    println("deviceUserDF...")
    val deviceUserDF = enrichDeviceWithMetadata(deviceDF, userDF)
    println("storageFuture...")
    val storageFuture = writeToStorage(deviceDF, storagePath)
    println("aggBybytesDF...")
    val aggBybytesDFUser = computeBytesTxRxUser(deviceUserDF)
    val aggBybytesDFApp = computeBytesTxRxApp(deviceUserDF)
    val aggBybytesDFAntenna = computeBytesTxRxAntenna(deviceUserDF)
    //val aggBybytesDF = computeBytesTxRx(aggBybytesDFUser, aggBybytesDFApp, aggBybytesDFAntenna)
    println("aggFuture...")
    val aggFutureUser = writeToJdbc(aggBybytesDFUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureApp = writeToJdbc(aggBybytesDFApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureAntenna = writeToJdbc(aggBybytesDFAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    println("Await...")

    Await.result(Future.sequence(Seq(aggFutureUser, aggFutureApp, aggFutureAntenna, storageFuture)), Duration.Inf)

    spark.close()
  }

}
