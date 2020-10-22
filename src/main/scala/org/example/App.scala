package org.example

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}
//import za.co.absa.abris.avro.functions.to_confluent_avro
//import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.functions.to_avro

/**
 * @author ${user.name}
 */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + "|" + b)

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    println("concat arguments = " + foo(args))

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("ETLJob")
      .getOrCreate()
    println("create dataframe")
    val tvalues: Array[Double] = Array(1.866393526974307, 2.864048126935307, 4.032486069215076, 7.876169953355888, 4.875333799256043, 14.316322626848278)
    val pvalues: Array[Double] = Array(0.064020056478447, 0.004808399479386827, 8.914865448939047E-5, 7.489564524121306E-13, 2.8363794106756046E-6, 0.0)
    val rdd = spark.sparkContext.parallelize(tvalues zip pvalues)
    import spark.implicits._
    val df = rdd.toDF("Tvalues","Pvalues")
    df.show
    val topic: String = "test123"
    /*
    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://schema-registry:8081"
    )
    val valueRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name"
      //, SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
    )
    val keyRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.name"
      //, SchemaManager.PARAM_KEY_SCHEMA_ID -> "latest"
    )
    var sparkOptions = Map("kafka.bootstrap.servers" -> "broker:29092")
    */
    var allColumns = struct(df.columns.head, df.columns.tail: _*)
    val tempdf = df.withColumn("allcolumns", allColumns)
    val schema = AvroSchemaUtils.toAvroSchema(tempdf, "allcolumns")
    println("avro schema = " + schema.toString())
    val toAvroConfig: ToAvroConfig = AbrisConfig
      .toConfluentAvro
      //.provideAndRegisterSchema(schema.toString())
      //.usingTopicNameStrategy(topic)
      .downloadSchemaByLatestVersion
      .andTopicNameStrategy(topic)
      .usingSchemaRegistry("http://schema-registry:8081")

    df.select(to_avro(allColumns, toAvroConfig) as 'value)
      .write
      .option("kafka.bootstrap.servers", "broker:29092")
      .option("topic", topic)
      .format("kafka")
      .save()
  }
}
