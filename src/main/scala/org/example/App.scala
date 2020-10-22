package org.example

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.abris.avro.functions.to_avro

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("ETLJob")
      .getOrCreate()
    println("create dataframe")
    val tvalues: Array[Double] = Array(1.8, 2.8, 4.0, 7.8, 4.8, 14.3)
    val pvalues: Array[Double] = Array(0.6, 0.4, 8.9, 7.4-13, 2.8, 0.0)
    val rdd = spark.sparkContext.parallelize(tvalues zip pvalues)
    import spark.implicits._
    val df = rdd.toDF("Tvalues","Pvalues")
    df.show
    val topic: String = "test123"
    var allColumns = struct(df.columns.head, df.columns.tail: _*)
    val tempdf = df.withColumn("allcolumns", allColumns)
    val schema = AvroSchemaUtils.toAvroSchema(tempdf, "allcolumns")
    println("avro schema = " + schema.toString())
    val toAvroConfig: ToAvroConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(schema.toString())
      .usingTopicNameStrategy(topic)
      .usingSchemaRegistry("http://schema-registry:8081")
    df.select(to_avro(allColumns, toAvroConfig) as 'value)
      .write
      .option("kafka.bootstrap.servers", "broker:29092")
      .option("topic", topic)
      .format("kafka")
      .save()
  }
}
