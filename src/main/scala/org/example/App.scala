package org.example

import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, Dataset}
import za.co.absa.abris.config._
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.avro.functions.from_avro


/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Please provide 2 arguments. The first argument is the name of the file to use as dataset to kafka. Set second argument to false if you don't want to do the stream join")
    } else {
      val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("ETLJob")
        .getOrCreate()
      val fileToRead = args(0);
      val doStreamJoin: Boolean = (args.length < 2) || (!(args(1).equalsIgnoreCase("false")))
      println("create first dataset")
      /*
      val kvalues: Array[Double] = Array(1.8, 2.8, 4.0, 7.8, 4.8, 14.3)
      val pvalues: Array[Double] = Array(0.6, 0.4, 8.9, 7.4, 2.8, 0.0)
      val rdd1 = spark.sparkContext.parallelize(kvalues zip pvalues)
      import spark.implicits._
      val dftokafka = rdd1.toDF("Kvalues", "Pvalues")
       */
      var dftokafka = spark.read.option("header", true)
        .csv(fileToRead)
      dftokafka.createOrReplaceTempView("dftokafka")

      println("create second dataset")
      /*
      val mvalues: Array[Double] = Array(0.6, 0.4, 8.9, 7.4, 2.8, 0.0)
      val rdd2 = spark.sparkContext.parallelize(kvalues zip mvalues)
      import spark.implicits._
      var df2 = rdd2.toDF("Kvalues", "Mvalues")
       */
      val df2 = spark.read.option("header", true)
        .csv("dataset2.csv")
      df2.createOrReplaceTempView("df2")

      println("write first dataset to kafka")
      dftokafka = spark.sql("SELECT named_struct('Kvalues', Kvalues, 'Pvalues', Pvalues) as value, uuid() as key, (SELECT COLLECT_SET(named_struct('hello', 'world'))) as headers FROM dftokafka")
      //dftokafka.printSchema()
      val topic: String = "ptopic"
      val schemaRegistry: String = "http://schema-registry:8081"
      val broker = "broker:29092"
      val subjectRecordNamespace = "MySubjectRecordNamespace"
      val subjectRecordName = "MySubjectRecordNames"
      val subjectNamingStrategy = "TopicRecordNameStrategy"
      //create the schema
      dataFrameToKafka(df = dftokafka, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSubjectNamingStrategy = subjectNamingStrategy, valueSubjectRecordNamespace = Some(subjectRecordNamespace), valueSubjectRecordName = Some(subjectRecordName))

      //write to the latest schema
      //dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSubjectNamingStrategy = "TopicRecordNameStrategy", valueSubjectRecordNamespace = Some("com.expediagroup.dataplatform"), valueSubjectRecordName = Some("PotentialRmdEntry"))
      //write to a specific schema
      //dataFrameToKafka(df = df, spark = spark, valueField = "value", topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, keyField = Some("key"), headerField = Some("headers"), valueSchemaVersion = Some(1), keySchemaVersion = Some(1), valueSubjectNamingStrategy = "TopicRecordNameStrategy", valueSubjectRecordNamespace = Some("com.expediagroup.dataplatform"), valueSubjectRecordName = Some("PotentialRmdEntry"))
      if (doStreamJoin) {
        //read kafka to dataframe
        println("read first dataset from kafka as a stream")
        val dfFromKafka = kafkaToDataFrame(spark = spark, topic = topic, kafkaBroker = broker, schemaRegistryUrl = schemaRegistry, subjectNamingStrategy = subjectNamingStrategy, subjectRecordNamespace = Some(subjectRecordNamespace), subjectRecordName = Some(subjectRecordName))
        dfFromKafka.createOrReplaceTempView("dfFromKafka")

        println("join streaming first dataset to static second dataset")
        val joinedDF = spark.sql("SELECT dfFromKafka.KValues, dfFromKafka.PValues, df2.MValues FROM dfFromKafka INNER JOIN df2 ON dfFromKafka.KValues = df2.KValues")

        //sink dataset to console
        val query = joinedDF.writeStream
          .outputMode("append")
          .format("console")
          .start()

        query.awaitTermination()
      }
    }
  }

  def kafkaToDataFrame(spark: SparkSession, topic: String, kafkaBroker: String, schemaRegistryUrl: String, subjectNamingStrategy: String = "TopicNameStrategy", subjectRecordName: Option[String] = None, subjectRecordNamespace: Option[String] = None): DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", topic)
      .option("schema.registry.url", schemaRegistryUrl)
      .option("startingoffsets", "earliest")
      .load()
    df.createOrReplaceTempView("df")
    println("****************************************************")
    val fromAvroConfig = GetFromAvroConfig(topic = topic, schemaRegistryUrl = schemaRegistryUrl, isKey = false, subjectNamingStrategy = subjectNamingStrategy, subjectRecordName = subjectRecordName, subjectRecordNamespace = subjectRecordNamespace)
    // val dft = spark.sql("select topic,value from df")
    df.printSchema()
    val dft = df.select(from_avro(df.col("value"), fromAvroConfig) as 'data).select("data.*")
    dft
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

  def GetFromAvroConfig(topic: String, schemaRegistryUrl: String, schemaVersion: Option[Int] = None, isKey: Boolean = false, subjectNamingStrategy: String = "TopicNameStrategy" /*other options are RecordNameStrategy, TopicRecordNameStrategy*/ , subjectRecordName: Option[String] = None, subjectRecordNamespace: Option[String] = None): FromAvroConfig = {
    //get the specified schema version
    //if not specified, then get the latest schema from Schema Registry
    //if the topic does not have a schema then create and register the schema
    //applies to both key and value
    val subject = if (subjectNamingStrategy.equalsIgnoreCase("TopicRecordNameStrategy")) SchemaSubject.usingTopicRecordNameStrategy(topicName = topic, recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else if (subjectNamingStrategy.equalsIgnoreCase("RecordNameStrategy")) SchemaSubject.usingRecordNameStrategy(recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else SchemaSubject.usingTopicNameStrategy(topicName = topic, isKey = isKey) // Use isKey=true for the key schema and isKey=false for the value schema
    val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryUrl)
    val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)
    println((if (isKey) "key" else "value") + " subject = " + subject.asString)
    var fromAvroConfig: FromAvroConfig = null
    val avroConfigFragment = AbrisConfig
      .fromConfluentAvro
    var fromStrategyConfigFragment: FromStrategyConfigFragment = null
    if (schemaVersion.isEmpty) {
      fromStrategyConfigFragment = avroConfigFragment.downloadReaderSchemaByLatestVersion
    }
    else {
      fromStrategyConfigFragment = avroConfigFragment.downloadReaderSchemaByVersion(schemaVersion.get)
    }

    val fromSchemaDownloadingConfigFragment = if (subjectNamingStrategy.equalsIgnoreCase("TopicRecordNameStrategy")) fromStrategyConfigFragment.andTopicRecordNameStrategy(topicName = topic, recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else if (subjectNamingStrategy.equalsIgnoreCase("RecordNameStrategy")) fromStrategyConfigFragment.andRecordNameStrategy(recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else fromStrategyConfigFragment.andTopicNameStrategy(topicName = topic, isKey = isKey)

    fromAvroConfig = fromSchemaDownloadingConfigFragment
      .usingSchemaRegistry(schemaRegistryUrl)

    println((if (isKey) "key" else "value") + " avro schema expected by schema registry  = " + fromAvroConfig.schemaString)
    fromAvroConfig
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
      val toSchemaDownloadingConfigFragment = if (subjectNamingStrategy.equalsIgnoreCase("TopicRecordNameStrategy")) toStrategyConfigFragment.andTopicRecordNameStrategy(topicName = topic, recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else if (subjectNamingStrategy.equalsIgnoreCase("RecordNameStrategy")) toStrategyConfigFragment.andRecordNameStrategy(recordName = subjectRecordName.getOrElse(""), recordNamespace = subjectRecordNamespace.getOrElse("")) else toStrategyConfigFragment.andTopicNameStrategy(topicName = topic, isKey = isKey)
      toAvroConfig = toSchemaDownloadingConfigFragment
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