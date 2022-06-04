package io.keepcoding.processing.batchlayer

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, value: Long, typ: String)

trait BatchLayer {
  // Inicio SparkSession
  val spark: SparkSession

  // Leo desde Storage
  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  // Leo información
  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  // Uno datos
  def enrichBytesWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  // Métricas agregadas cada hora (Total de bytes {rx_antenna, tx_user, tx_app} e emails > cuota)
  def computeBytesTxRx(dataFrame: DataFrame): DataFrame
  // Métricas agregadas cada hora (Total de bytes e emails > cuota)
  def userQuotaLimitDF(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcBytesTable, aggJdbcQuotaTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val bytesDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val bytesMetadataDF = enrichBytesWithMetadata(bytesDF, metadataDF).cache()
    val computeBytesDF = computeBytesTxRx(bytesDF)
    val userQuotaLimitDF = userQuotaLimitDF(bytesMetadataDF)

    writeToJdbc(computeBytesDF, jdbcUri, aggJdbcBytesTable, jdbcUser, jdbcPassword)
    writeToJdbc(userQuotaLimitDF, jdbcUri, aggJdbcQuotaTable, jdbcUser, jdbcPassword)

    spark.close()
  }

}

