package org.example

import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils.toAvroSchema
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject

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
    val pvalues: Array[Double] = Array(0.6, 0.4, 8.9, 7.4, 2.8, 0.0)
    val rdd = spark.sparkContext.parallelize(tvalues zip pvalues)
    import spark.implicits._
    val df = rdd.toDF("Tvalues","Pvalues")
    df.show
    val topic: String = "test123"
    val schemaRegistry: String = "http://schema-registry:8081"
    var allColumns = struct(df.columns.head, df.columns.tail: _*)
    //val expression = allColumns.expr
    //val schema = toAvroType(expression.dataType, expression.nullable)
    val tempdf = df.withColumn("allcolumns", allColumns)
    val schema = toAvroSchema(tempdf, "allcolumns")
    println("avro schema = " + schema.toString())

    val subject = SchemaSubject.usingTopicNameStrategy(topic, isKey=false) // Use isKey=true for the key schema and isKey=false for the value schema
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistry)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val schemaId = schemaManager.register(subject, schema)
    val toAvroConfig: ToAvroConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(schemaRegistry)
    df.select(to_avro(allColumns, toAvroConfig) as 'value)
      .write
      .option("kafka.bootstrap.servers", "broker:29092")
      .option("topic", topic)
      .format("kafka")
      .save()
  }
}
