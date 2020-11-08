# simplespark

## Run locally

> please note that this repo was crearted to share an issue I'm facing. Hence the following instructions are to reproduce the issue. Until the issue ios resolved, you won't see data being moved

- Stand up a dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml up -d --build
```

- Build an image for a dockerised spark server
```shell script
docker build -f ./docker_spark_server/Dockerfile -t spark3.0.0-scala2.12-hadoop3.2.1 ./docker_spark_server
```

- Build the spark application
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn clean install
```
- Run the Spark application using the dockerised spark server
#### Using the JAR with dependencies
```shell script
# Use either ONE of the following commands

docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.0-scala2.12-hadoop3.2.1:latest spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT-jar-with-dependencies.jar

docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.0-scala2.12-hadoop3.2.1:latest spark-submit --repositories https://packages.confluent.io/maven  --packages za.co.absa:abris_2.12:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT.jar
```

- Open the url http://http://localhost:9021/clusters and, using the menu options on the left, navigate to the "Topics" screen

### Ideal result

The spark application will create a topic `test123`, register the schema of a Spark dataframe with two numeric fields, and then write the records of the dataframe to the topic `test123`. You should be able to see the topic `test123` on the Topics screen of Confluent Control Center at http://localhost:9021/ ; and you should be ab le browse through the messages written into the topic. 

### What's stopping us?

The app is able to create the Kafka topic and register the schema if provided (more about this below); but is unable to write messages to the topic. The use of Hadoop 3.x to run the App seems to be the problem. 

The big bad error is 
```text
Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 12 in stage 3.0 failed 1 times, most recent failure: Lost task 12.0 in stage 3.0 (TID 28, f51cc3ba3b65, executor driver): java.lang.N
oSuchMethodError: org.apache.kafka.clients.producer.KafkaProducer.flush()V
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
        at org.apache.spark.SparkContext.$anonfun$runJob$5(SparkContext.scala:2133)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:127)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:444)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1377)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:447)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2023)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:1972)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:1971)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1971)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:950)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:950)
        at scala.Option.foreach(Option.scala:407)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:950)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2203)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2152)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2141)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:752)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2093)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2114)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2133)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2158)
        at org.apache.spark.rdd.RDD.$anonfun$foreachPartition$1(RDD.scala:994)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
        at org.apache.spark.rdd.RDD.withScope(RDD.scala:388)
        at org.apache.spark.rdd.RDD.foreachPartition(RDD.scala:992)
        at org.apache.spark.sql.kafka010.KafkaWriter$.write(KafkaWriter.scala:70)
        at org.apache.spark.sql.kafka010.KafkaSourceProvider.createRelation(KafkaSourceProvider.scala:180)
        at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:46)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:90)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:175)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:213)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:210)
        at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:171)
        at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:122)
        at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:121)
        at org.apache.spark.sql.DataFrameWriter.$anonfun$runCommand$1(DataFrameWriter.scala:944)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$5(SQLExecution.scala:100)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:160)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:87)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
        at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:944)
        at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:396)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:380)
        at org.example.App$.main(App.scala:46)
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
Caused by: java.lang.NoSuchMethodError: org.apache.kafka.clients.producer.KafkaProducer.flush()V
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
        at org.apache.spark.SparkContext.$anonfun$runJob$5(SparkContext.scala:2133)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:127)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:444)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1377)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:447)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
```

Another thing this version of ABRiS doesn't seem to be able to do, is to generate the Avro schema off the dataframe and register it.

You can provide an Avro schema to register to the topic (as shown in this branch); and it registers okay, but it does throw a non-fatal error prior to registering
```text
20/11/08 05:16:32 ERROR confluent.SchemaManager: Problems found while retrieving metadata for subject 'test123-value'
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject 'test123-value' not found.; error code: 40401
        at io.confluent.kafka.schemaregistry.client.rest.RestService.sendHttpRequest(RestService.java:230)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.httpRequest(RestService.java:256)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.getLatestVersion(RestService.java:515)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.getLatestVersion(RestService.java:507)
        at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.getLatestSchemaMetadata(CachedSchemaRegistryClient.java:275)
        at za.co.absa.abris.avro.read.confluent.SchemaManager$$anonfun$5.apply(SchemaManager.scala:123)
        at za.co.absa.abris.avro.read.confluent.SchemaManager$$anonfun$5.apply(SchemaManager.scala:123)
        at scala.util.Try$.apply(Try.scala:192)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.exists(SchemaManager.scala:123)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.register(SchemaManager.scala:110)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.register(SchemaManager.scala:100)
        at org.example.App$.main(App.scala:40)
        at org.example.App.main(App.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:845)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:161)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:184)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:86)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:920)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:929)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml down
```