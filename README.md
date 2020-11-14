# simplespark

## Run locally

> please note that this repo was crearted to share an issue I'm facing. Hence the following instructions are to reproduce the issue. Until the issue ios resolved, you won't see data being moved

- Stand up a dockerised kafka cluster by running 
```shell script
CONFLUENT_VERSION="5.5.2" docker-compose -f docker_kafka_server/docker-compose.yml up -d --build
```

- Build an image for a dockerised spark server
```shell script
docker build -f ./docker_spark_server/Dockerfile -t spark3.0.1-scala2.12-hadoop2.10.0 ./docker_spark_server
```

- Build the spark application
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn clean install
```
- Run the Spark application using the dockerised spark server
```shell script
# Use either ONE of the following commands
# ...using JAR with dependencies...
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop2.10.0:latest spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT-jar-with-dependencies.jar
# ... OR, using JAR without dependencies...
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop2.10.0:latest spark-submit --repositories https://packages.confluent.io/maven --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,za.co.absa:abris_2.12:4.0.1 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT.jar
```
- Open the url http://http://localhost:9021/clusters and, using the menu options on the left, navigate to the "Topics" screen

### Ideal result

The spark application will create a topic `test123`, register the schema of a Spark dataframe with two numeric fields, and then write the records of the dataframe to the topic `test123`. You should be able to see the topic `test123` on the Topics screen of Confluent Control Center at http://localhost:9021/ ; and you should be ab le browse through the messages written into the topic. 

### What's stopping us?

Nothing; it all works!

There are some minor issues. 

#### Minor Issue 1

If you use 
```scala
  val expression = allColumns.expr
  toAvroType(expression.dataType, expression.nullable)
```
to generate the avro schema for the struct of all columns in the dataframe as suggested at https://github.com/AbsaOSS/ABRiS/blob/master/documentation/confluent-avro-documentation.md#generate-schema-from-data-and-register, then you will get the following runtime error
```text
Exception in thread "main" java.lang.UnsupportedOperationException: Cannot evaluate expression: NamePlaceholder
        at org.apache.spark.sql.catalyst.expressions.Unevaluable.eval(Expression.scala:301)
        at org.apache.spark.sql.catalyst.expressions.Unevaluable.eval$(Expression.scala:300)
        at org.apache.spark.sql.catalyst.expressions.NamePlaceholder$.eval(complexTypeCreator.scala:302)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.$anonfun$names$1(complexTypeCreator.scala:377)
        at scala.collection.immutable.List.map(List.scala:286)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.names$lzycompute(complexTypeCreator.scala:377)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.names(complexTypeCreator.scala:377)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.dataType$lzycompute(complexTypeCreator.scala:384)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.dataType(complexTypeCreator.scala:383)
        at org.apache.spark.sql.catalyst.expressions.CreateNamedStruct.dataType(complexTypeCreator.scala:372)
        at org.example.App$.main(App.scala:34)
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

##### Workaround
Use the following script instead, to generate the schema (ref )

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
CONFLUENT_VERSION="5.5.2" docker-compose -f docker_kafka_server/docker-compose.yml down
```