package io.keepcoding.processing.batchlayer

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BatchLayerImpl extends BatchLayer {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  // Leo desde nuestro archivo guardado en formato parquet
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

  // Leemos metadatos desde nuestra base de datos
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

  // Unimos los datos y metadatos
  override def enrichBytesWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  // Métricas agregadas cada hora (Total de bytes {rx_antenna, tx_user, tx_app} e emails > cuota)

  override def computeBytesTxRx(dataFrame: DataFrame): DataFrame = {
    // Total de bytes recibidos por antena
    dataFrame
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .withColumn("timestamp", $"window.start")
      .withColumn("id", $"antenna_id")
      .withColumn("type", lit("antenna_bytes_total"))
      .select($"timestamp", $"id", $"value", $"type")
  }

  override def userQuotaLimitDF(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(
        $"id",
        $"name",
        $"email",
        $"quota",
        $"bytes"
      ).withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("usage"))
      .withColumn("timestamp", $"window.start")
      .select($"email", $"usage", $"quota", $"timestamp")
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

  def main(args: Array[String]): Unit = run(args)
}
