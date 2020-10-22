# simplespark

## Run locally

> please note that this repo was crearted to share an issue I'm facing. Hence the following instructions are to reproduce the issue. Until the issue ios resolved, you won't see data being moved

- Stand up a dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml up -d --build
```

- Build an image for a dockerised spark server
```shell script
docker build -f ./docker_spark_server/Dockerfile -t spark3.0.1-scala2.12-hadoop3.2.1 ./docker_spark_server
```

- Build the spark application
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn clean install
```
- Run the Spark application using the dockerised spark server
```shell script
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop3.2.1:latest spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT-jar-with-dependencies.jar
```
- Open the url http://http://localhost:9021/clusters and, using the menu options on the left, navigate to the "Topics" screen

### Ideal result

The spark application will create a topic `test123`, register the schema of a Spark dataframe with two numeric fields, and then write the records of the dataframe to the topic `test123`. You should be able to see the topic `test123` on the Topics screen of Confluent Control Center at http://localhost:9021/ ; and you should be ab le browse through the messages written into the topic. 

### What's stopping us?

The spark application fails during runtime, with the following error 
```text
2020-10-22 20:58:06,963 INFO confluent.SchemaManager: AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: za.co.absa.abris.avro.registry.SchemaSubject@1f3361e9
Exception in thread "main" java.lang.NoSuchFieldError: FACTORY
        at org.apache.avro.Schemas.toString(Schemas.java:36)
        at org.apache.avro.Schemas.toString(Schemas.java:30)
        at io.confluent.kafka.schemaregistry.avro.AvroSchema.canonicalString(AvroSchema.java:140)
        at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.registerAndGetId(CachedSchemaRegistryClient.java:206)
        at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.register(CachedSchemaRegistryClient.java:268)
        at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.register(CachedSchemaRegistryClient.java:244)
        at io.confluent.kafka.schemaregistry.client.SchemaRegistryClient.register(SchemaRegistryClient.java:42)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.register(SchemaManager.scala:77)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.$anonfun$getIfExistsOrElseRegisterSchema$1(SchemaManager.scala:124)
        at scala.runtime.java8.JFunction0$mcI$sp.apply(JFunction0$mcI$sp.java:23)
        at scala.Option.getOrElse(Option.scala:189)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.getIfExistsOrElseRegisterSchema(SchemaManager.scala:124)
        at za.co.absa.abris.config.ToSchemaRegisteringConfigFragment.usingSchemaRegistry(Config.scala:135)
        at za.co.absa.abris.config.ToSchemaRegisteringConfigFragment.usingSchemaRegistry(Config.scala:131)
        at org.example.App$.main(App.scala:37)
        at org.example.App.main(App.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:928)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

You can work around this error, by createing the topic `test123` manually using the Confluent Control Center, and setting the topic's value schema to 
```json
{"type":"record","name":"topLevelRecord","fields":[{"name":"Tvalues","type":"double"},{"name":"Pvalues","type":"double"}]}
``` 
Then , you can replace the code in App.scala 
```scala
    .provideAndRegisterSchema(schema.toString())
    .usingTopicNameStrategy(topic)
```
with 
```scala
    .downloadSchemaByLatestVersion
    .andTopicNameStrategy(topic)
```

However, when you re-run , you will get a new error 
```text
java.lang.NoSuchMethodError: org.apache.kafka.clients.producer.KafkaProducer.flush()V
        at org.apache.spark.sql.kafka010.KafkaWriteTask.$anonfun$close$1(KafkaWriteTask.scala:61)
        at org.apache.spark.sql.kafka010.KafkaWriteTask.$anonfun$close$1$adapted(KafkaWriteTask.scala:60)
        at scala.Option.foreach(Option.scala:407)
        at org.apache.spark.sql.kafka010.KafkaWriteTask.close(KafkaWriteTask.scala:60)
        at org.apache.spark.sql.kafka010.KafkaWriter$.$anonfun$write$3(KafkaWriter.scala:73)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1386)
        at org.apache.spark.sql.kafka010.KafkaWriter$.$anonfun$write$1(KafkaWriter.scala:73)
        at org.apache.spark.sql.kafka010.KafkaWriter$.$anonfun$write$1$adapted(KafkaWriter.scala:70)
        at org.apache.spark.rdd.RDD.$anonfun$foreachPartition$2(RDD.scala:994)
        at org.apache.spark.rdd.RDD.$anonfun$foreachPartition$2$adapted(RDD.scala:994)
        at org.apache.spark.SparkContext.$anonfun$runJob$5(SparkContext.scala:2139)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:127)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:446)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1377)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:449)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
```
...and there will be no records written to the topic, either. 

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml down
```