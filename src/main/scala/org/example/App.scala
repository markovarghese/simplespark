package org.example

import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig, ToStrategyConfigFragment}
import za.co.absa.abris.avro.functions.to_avro
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
    var df = rdd.toDF("Tvalues", "Pvalues")
    df.createOrReplaceTempView("df")
    df = spark.sql("SELECT named_struct('Tvalues', Tvalues, 'Pvalues', Pvalues) as value, uuid() as key, (SELECT COLLECT_SET(named_struct('hello', 'world'))) as headers FROM df")
    df.show
    val topic: String = "test123"
    val schemaRegistry: String = "http://schema-registry:8081"
    val broker = "broker:29092"
    //create the schema
    dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"))
    //write to the latest schema
    dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"))
    //write to a specifci schema
    dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSchemaVersion = Some(1), keySchemaVersion = Some(1))
  }

  def dataFrameToKafka(spark: SparkSession, df: DataFrame, valueField: String, topic: String, kafkaBroker: String, schemaRegistryUrl: String, valueSchemaVersion: Option[Int] = None, keyField: Option[String] = None, keySchemaVersion: Option[Int] = None, headerField: Option[String] = None): Unit = {
    var dfavro = spark.emptyDataFrame
    var columnsToSelect = Seq(to_avro(df.col(valueField), GetToAvroConfig(topic = topic, schemaRegistryUrl = schemaRegistryUrl, dfColumn = df.col(valueField), schemaVersion = valueSchemaVersion, isKey = false)) as 'value)
    if (!keyField.isEmpty) {
      val keyFieldCol = df.col(keyField.get)
      columnsToSelect = columnsToSelect ++ Seq(to_avro(keyFieldCol, GetToAvroConfig(topic = topic, schemaRegistryUrl = schemaRegistryUrl, dfColumn = keyFieldCol, schemaVersion = keySchemaVersion, isKey = true)) as 'key)
    }
    if (!headerField.isEmpty) {
      columnsToSelect = columnsToSelect ++ Seq(df.col(headerField.get) as 'header)
    }
    dfavro = df.select(columnsToSelect: _*)
    dfavro.printSchema()
    dfavro.write
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", topic)
      .option("includeHeaders", (!headerField.isEmpty).toString)
      .format("kafka")
      .save()
  }

  def GetToAvroConfig(topic: String, schemaRegistryUrl: String, dfColumn: Column, schemaVersion: Option[Int] = None, isKey: Boolean = false): ToAvroConfig = {
    //get the specified schema version
    //if not specified, then get the latest schema from Schema Registry
    //if the topic does not have a schema then create and register the schema
    //applies to both key and value
    val subject = SchemaSubject.usingTopicNameStrategy(topic, isKey = isKey) // Use isKey=true for the key schema and isKey=false for the value schema
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    var toAvroConfig: ToAvroConfig = null
    if (schemaManager.exists(subject)) {
      val avroConfigFragment = AbrisConfig
        .toConfluentAvro
      var toStrategyConfigFragment: ToStrategyConfigFragment = null
      if (schemaVersion.isEmpty) {
        toStrategyConfigFragment = avroConfigFragment.downloadSchemaByLatestVersion
      }
      else {
        toStrategyConfigFragment = avroConfigFragment.downloadSchemaByVersion(schemaVersion.get)
      }
      toAvroConfig = toStrategyConfigFragment
        .andTopicNameStrategy(topic, isKey = isKey)
        .usingSchemaRegistry(schemaRegistryUrl)
    }
    else {
      val expression = dfColumn.expr
      val schema = toAvroType(expression.dataType, expression.nullable)
      println("avro schema = " + schema.toString())
      val schemaId = schemaManager.register(subject, schema)
      toAvroConfig = AbrisConfig
        .toConfluentAvro
        .downloadSchemaById(schemaId)
        .usingSchemaRegistry(schemaRegistryUrl)
    }
    toAvroConfig
  }
}