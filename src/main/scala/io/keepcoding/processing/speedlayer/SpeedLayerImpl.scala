package io.keepcoding.processing.speedlayer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object SpeedLayerImpl extends  SpeedLayer {
  // Primero construimos la estancia SparkSession
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  // Leo desde Kafka
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  // Parseo (Json a Dataframe)  | timestamp | id | antenna_id | bytes | app |
  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    // Definimos el esquema (Información de nos llega de la Antena)
    val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable=false),
      StructField("id", StringType, nullable=false),
      StructField("antenna_id", StringType, nullable=false),
      StructField("app", StringType, nullable=false),
      StructField("bytes", LongType, nullable=false)
    ))
    // parseamos y desencapsulamos json
    dataFrame
      .select(from_json(col("value").cast(StringType), jsonSchema).as("json"))
      .select("json.*")
  }

  // Leo información de usuarios (Conexión a la base de datos)
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

  // Uno información de dispositivos con usuarios
  override def enrichDeviceWithMetadata(devicesDF: DataFrame, usersDF: DataFrame): DataFrame = {
    devicesDF.join(usersDF, Seq("id"))
  }

  // Métricas agregadas cada 5 minutos (Total de bytes {rx_antenna, tx_user, tx_app})
  // rx_antena
  override def computeBytesTxRxAntenna(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .select(
        $"id".as("user_id"),
        $"antenna_id",
        $"app".as("app_id"),
        $"timestamp",
        $"bytes"
      ).withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minute"), $"antenna_id")
      .agg(sum("bytes").as("value"))
      .withColumn("timestamp", $"window.start")
      .withColumn("id", $"antenna_id")
      .withColumn("type", lit("antenna_bytes_total"))
      .select($"timestamp", $"id", $"value", $"type")
  }
  // tx_user
  override def computeBytesTxRxUser(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    /*
      TABLE bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
    */
    dataFrame
      .select(
        $"id".as("user_id"),
        $"antenna_id",
        $"app".as("app_id"),
        $"timestamp",
        $"bytes"
      ).withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minute"), $"antenna_id")
      .agg(sum("bytes").as("value"))
      .withColumn("timestamp", $"window.start")
      .withColumn("id", $"antenna_id")
      .withColumn("type", lit("antenna_bytes_total"))
      .select($"timestamp", $"id", $"value", $"type")
  }
  // tx_app
  override def computeBytesTxRxApp(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    /*
      TABLE bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
    */
    dataFrame
      .select(
        $"id".as("user_id"),
        $"antenna_id",
        $"app".as("app_id"),
        $"timestamp",
        $"bytes"
      ).withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minute"), $"app_id")
      .agg(sum("bytes").as("value"))
      .withColumn("timestamp", $"window.start")
      .withColumn("id", $"app_id")
      .withColumn("type", lit("app_bytes_total"))
      .select($"timestamp", $"id", $"value", $"type")
  }
  override def computeBytesTxRx(userDF: DataFrame, appDF: DataFrame, antennaDF: DataFrame): DataFrame = {
    userDF.toDF().union(antennaDF.toDF())
  }
  // Guarda en PortgreSQL Por cada Batch - (En paralelo con writeToStorage - Future)
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    println("writeToJdbc")
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

  // Guarda en GC_Storage Por cada Batch - (En paralelo con writeToJdbc - Future)
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    import dataFrame.sparkSession.implicits._
    println("writeToStorage")
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )
    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)
}
