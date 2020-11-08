# simplespark

## Run locally

> please note that this repo was crearted to share an issue I'm facing. Hence the following instructions are to reproduce the issue. Until the issue ios resolved, you won't see data being moved

- Stand up a dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml up -d --build
```

- Build an image for a dockerised spark server
```shell script
docker build -f ./docker_spark_server/Dockerfile -t spark3.0.0-scala2.12-hadoop2.10.0 ./docker_spark_server
```

- Build the spark application
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn clean install
```
- Run the Spark application using the dockerised spark server
#### Using the JAR with dependencies
```shell script
# Use either ONE of the following commands

docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.0-scala2.12-hadoop2.10.0:latest spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT-jar-with-dependencies.jar

docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.0-scala2.12-hadoop2.10.0:latest spark-submit --repositories https://packages.confluent.io/maven  --packages za.co.absa:abris_2.12:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT.jar
```

- Open the url http://http://localhost:9021/clusters and, using the menu options on the left, navigate to the "Topics" screen

### Ideal result

The spark application will create a topic `test123`, register the schema of a Spark dataframe with two numeric fields, and then write the records of the dataframe to the topic `test123`. You should be able to see the topic `test123` on the Topics screen of Confluent Control Center at http://localhost:9021/ ; and you should be ab le browse through the messages written into the topic. 

### What's stopping us?

Nothing major. The only thing this version of ABRiS doesn't seem to be able to do, is to generate the Avro schema off the dataframe and register it.

If writing to a topic with a schema already registered , everything is fine.

You can provide an Avro schema to register to the topic (as shown in this branch); and it registers okay, but it does throw a non-fatal error prior to registering
```text
20/11/08 07:20:32 ERROR confluent.SchemaManager: Problems found while retrieving metadata for subject 'test123-value'
io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Subject 'test123-value' not found.; error code: 40401
        at io.confluent.kafka.schemaregistry.client.rest.RestService.sendHttpRequest(RestService.java:230)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.httpRequest(RestService.java:256)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.getLatestVersion(RestService.java:515)
        at io.confluent.kafka.schemaregistry.client.rest.RestService.getLatestVersion(RestService.java:507)
        at io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.getLatestSchemaMetadata(CachedSchemaRegistryClient.java:275)
        at za.co.absa.abris.avro.read.confluent.SchemaManager.$anonfun$exists$1(SchemaManager.scala:123)
        at scala.util.Try$.apply(Try.scala:213)
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
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:928)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml down
```