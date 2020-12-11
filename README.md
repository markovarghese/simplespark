# simplespark

## Run locally

- Stand up a dockerised kafka cluster by running 
```shell script
CONFLUENT_VERSION="5.5.2" docker-compose -f docker_kafka_server/docker-compose.yml up -d --build
```

- Build an image for a dockerised spark server
```shell script
docker build -f ./docker_spark_server/Dockerfile -t spark2.4.6-scala2.11-hadoop2.10.0 ./docker_spark_server
```

- Build the spark application
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn clean install
```

- Create folders to watch for more dataset1 data, and archive once read
```shell script
mkdir csvfolder
mkdir archivefolder
```

- Run the Spark application using the dockerised spark server
```shell script
# Use either ONE of the following commands
# ...using JAR with dependencies...
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark2.4.6-scala2.11-hadoop2.10.0:latest spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT-jar-with-dependencies.jar
# ... OR, using JAR without dependencies...
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark2.4.6-scala2.11-hadoop2.10.0:latest spark-submit --repositories https://packages.confluent.io/maven --packages io.confluent:kafka-schema-registry-client:5.5.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6,za.co.absa:abris_2.11:4.0.1 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT.jar
```

- Open the url http://http://localhost:9021/clusters and, using the menu options on the left, navigate to the "Topics" screen

- On a second terminal, run 
```shell script
cp moredataset1.csv csvfolder/dataset1.csv
```

- Watch topic "ptopic" get the data in the file dataset1.csv

- Watch topic "pmtopic" get the batch-to-stream join of the topic ptopic with the data in dataset2.csv

- On a second terminal, run
```shell script
cp moredataset1.csv csvfolder/moredataset1.csv
```

- Watch the `pmtopic` topic to view the join of the new data in the topic `ptopic` (from moredataset1.csv that was detected by spark) with the data in dataset2.csv

### Ideal result

This demo does the following...
1. Write any new/changed CSV file to a kafka topic `ptopic` 
1. Read `ptopic` as a stream 
1. Join the stream to the batch data in `dataset2.csv`
1. Manually put data from `dataset1.csv` into the topic `ptopic`
1. The main application join the new data as it comes into the topic `pmtopic`
1. Manually put more data from `moredataset1.csv` into the topic `ptopic`
1. Watch both streams update

### What's stopping us?

Nothing; it all works!

#### Minor Issue
If you run the Spark application on a spark cluster with Hadoop 3.0.0 to 3.2.1, you will get the following runtime error
```text
2020-11-14 23:32:33,626 ERROR executor.Executor: Exception in task 6.0 in stage 3.0 (TID 22)
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

##### Workaround
Run the spark application on a spark cluster with Hadoop 3.3.0 or Hadoop 2.x (I've tested successfully with 2.8.5 and 2.10.0)

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
CONFLUENT_VERSION="5.5.2" docker-compose -f docker_kafka_server/docker-compose.yml down
```
Cleanup files created by this demo by running
```shell
sudo chown -R $(whoami):$(whoami) .
rm -rf archivefolder
rm -rf csvfolder
rm -rf checkpointfolder
```

> Use `sudo chown -R $(whoami):$(whoami) .` to get access to folders/files created by docker