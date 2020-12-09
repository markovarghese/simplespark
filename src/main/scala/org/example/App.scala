package org.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils}
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig, ToStrategyConfigFragment}
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


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
    df.printSchema()
    val topic: String = "test123"
    val schemaRegistry: String = "http://schema-registry:8081"
    val broker = "broker:29092"
    //create the schema
    dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSubjectNamingStrategy = "TopicRecordNameStrategy", valueSubjectRecordNamespace = Some("com.expediagroup.dataplatform"), valueSubjectRecordName = Some("PotentialRmdEntry"))
    //write to the latest schema
    //dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSubjectNamingStrategy = "TopicRecordNameStrategy", valueSubjectRecordNamespace = Some("com.expediagroup.dataplatform"), valueSubjectRecordName = Some("PotentialRmdEntry"))
    //write to a specific schema
    //dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSchemaVersion = Some(1), keySchemaVersion = Some(1), valueSubjectNamingStrategy = "TopicRecordNameStrategy", valueSubjectRecordNamespace = Some("com.expediagroup.dataplatform"), valueSubjectRecordName = Some("PotentialRmdEntry"))

    //read kafka to dstream

    //sink dstream to a file
  }

  def kafkaToDStream (spark: SparkSession,topic: String,kafkaBroker: String, schemaRegistryUrl: String) : Unit= {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafkaBroker",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("topic")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(record => (record.key, record.value))

  }

  def dataFrameToKafka(spark: SparkSession, df: DataFrame, valueField: String, topic: String, kafkaBroker: String, schemaRegistryUrl: String, valueSchemaVersion: Option[Int] = None, valueSubjectNamingStrategy: String = "TopicNameStrategy" /*other options are RecordNameStrategy, TopicRecordNameStrategy*/ , valueSubjectRecordName: Option[String] = None, valueSubjectRecordNamespace: Option[String] = None, keyField: Option[String] = None, keySchemaVersion: Option[Int] = None, keySubjectNamingStrategy: String = "TopicNameStrategy" /*other options are RecordNameStrategy, TopicRecordNameStrategy*/ , keySubjectRecordName: Option[String] = None, keySubjectRecordNamespace: Option[String] = None, headerField: Option[String] = None): Unit = {
    var dfavro = spark.emptyDataFrame
    var columnsToSelect = Seq(to_avro(df.col(valueField), GetToAvroConfig(topic = topic, schemaRegistryUrl = schemaRegistryUrl, dfColumn = df.col(valueField), schemaVersion = valueSchemaVersion, isKey = false, subjectNamingStrategy = valueSubjectNamingStrategy, subjectRecordName = valueSubjectRecordName, subjectRecordNamespace = valueSubjectRecordNamespace)) as 'value)
    if (!keyField.isEmpty) {
      val keyFieldCol = df.col(keyField.get)
      columnsToSelect = columnsToSelect ++ Seq(to_avro(keyFieldCol, GetToAvroConfig(topic = topic, schemaRegistryUrl = schemaRegistryUrl, dfColumn = keyFieldCol, schemaVersion = keySchemaVersion, isKey = true, subjectNamingStrategy = keySubjectNamingStrategy, subjectRecordName = keySubjectRecordName, subjectRecordNamespace = keySubjectRecordNamespace)) as 'key)
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

  def GetToAvroConfig(topic: String, schemaRegistryUrl: String, dfColumn: Column, schemaVersion: Option[Int] = None, isKey: Boolean = false, subjectNamingStrategy: String = "TopicNameStrategy" /*other options are RecordNameStrategy, TopicRecordNameStrategy*/ , subjectRecordName: Option[String] = None, subjectRecordNamespace: Option[String] = None): ToAvroConfig = {
    //get the specified schema version
    //if not specified, then get the latest schema from Schema Registry
    //if the topic does not have a schema then create and register the schema
    //applies to both key and value
    val subject = if (subjectNamingStrategy.equalsIgnoreCase("TopicRecordNameStrategy")) SchemaSubject.usingTopicRecordNameStrategy(topicName = topic, recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else if (subjectNamingStrategy.equalsIgnoreCase("RecordNameStrategy")) SchemaSubject.usingRecordNameStrategy(recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else SchemaSubject.usingTopicNameStrategy(topicName = topic, isKey = isKey) // Use isKey=true for the key schema and isKey=false for the value schema
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    val expression = dfColumn.expr
    val dataSchema = toAvroType(expression.dataType, expression.nullable)
    println((if (isKey) "key" else "value") + " subject = " + subject.asString)
    println((if (isKey) "key" else "value") + " avro schema inferred from data  = " + dataSchema.toString())
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
      val schemaId = schemaManager.register(subject, dataSchema)
      toAvroConfig = AbrisConfig
        .toConfluentAvro
        .downloadSchemaById(schemaId)
        .usingSchemaRegistry(schemaRegistryUrl)
    }
    println((if (isKey) "key" else "value") + " avro schema expected by schema registry  = " + toAvroConfig.schemaString)
    toAvroConfig
  }
}