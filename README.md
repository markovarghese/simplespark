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

#### Maven Dependency Tree
If you run 
```shell script
docker run -e MAVEN_OPTS="-Xmx1024M -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled" --rm -v "${PWD}":/usr/src/mymaven -v "${HOME
}/.m2":/root/.m2 -w /usr/src/mymaven maven:3.6.3-jdk-8 mvn dependency:tree
```

you will get the following dependency tree
```text
[INFO] ----------------------< org.example:simplespark >-----------------------
[INFO] Building simplespark 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ simplespark ---
[INFO] org.example:simplespark:jar:1.0-SNAPSHOT
[INFO] +- za.co.absa:abris_2.12:jar:4.0.0:compile
[INFO] |  +- org.apache.avro:avro:jar:1.9.2:compile
[INFO] |  |  +- com.fasterxml.jackson.core:jackson-core:jar:2.10.2:compile
[INFO] |  |  +- org.apache.commons:commons-compress:jar:1.19:compile
[INFO] |  |  \- org.slf4j:slf4j-api:jar:1.7.25:compile
[INFO] |  +- org.apache.spark:spark-avro_2.12:jar:2.4.6:compile
[INFO] |  +- io.confluent:kafka-avro-serializer:jar:5.5.1:compile
[INFO] |  |  \- io.confluent:kafka-schema-serializer:jar:5.5.1:compile
[INFO] |  +- io.confluent:kafka-schema-registry-client:jar:5.5.1:compile
[INFO] |  |  +- org.apache.kafka:kafka-clients:jar:5.5.1-ccs:compile
[INFO] |  |  +- javax.ws.rs:javax.ws.rs-api:jar:2.1.1:compile
[INFO] |  |  +- org.glassfish.jersey.core:jersey-common:jar:2.30:compile
[INFO] |  |  |  +- jakarta.ws.rs:jakarta.ws.rs-api:jar:2.1.6:compile
[INFO] |  |  |  +- jakarta.annotation:jakarta.annotation-api:jar:1.3.5:compile
[INFO] |  |  |  +- org.glassfish.hk2.external:jakarta.inject:jar:2.6.1:compile
[INFO] |  |  |  \- org.glassfish.hk2:osgi-resource-locator:jar:1.0.3:compile
[INFO] |  |  \- io.swagger:swagger-annotations:jar:1.6.0:compile
[INFO] |  +- io.confluent:common-config:jar:5.5.1:compile
[INFO] |  \- io.confluent:common-utils:jar:5.5.1:compile
[INFO] \- org.apache.spark:spark-sql_2.12:jar:3.0.0:provided
[INFO]    +- com.univocity:univocity-parsers:jar:2.8.3:provided
[INFO]    +- org.apache.spark:spark-sketch_2.12:jar:3.0.0:provided
[INFO]    +- org.apache.spark:spark-core_2.12:jar:3.0.0:provided
[INFO]    |  +- com.thoughtworks.paranamer:paranamer:jar:2.8:provided
[INFO]    |  +- org.apache.avro:avro-mapred:jar:hadoop2:1.8.2:provided
[INFO]    |  |  \- org.apache.avro:avro-ipc:jar:1.8.2:provided
[INFO]    |  +- com.twitter:chill_2.12:jar:0.9.5:provided
[INFO]    |  |  \- com.esotericsoftware:kryo-shaded:jar:4.0.2:provided
[INFO]    |  |     +- com.esotericsoftware:minlog:jar:1.3.0:provided
[INFO]    |  |     \- org.objenesis:objenesis:jar:2.5.1:provided
[INFO]    |  +- com.twitter:chill-java:jar:0.9.5:provided
[INFO]    |  +- org.apache.hadoop:hadoop-client:jar:2.7.4:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-common:jar:2.7.4:provided
[INFO]    |  |  |  +- commons-cli:commons-cli:jar:1.2:provided
[INFO]    |  |  |  +- xmlenc:xmlenc:jar:0.52:provided
[INFO]    |  |  |  +- commons-httpclient:commons-httpclient:jar:3.1:provided
[INFO]    |  |  |  +- commons-io:commons-io:jar:2.4:provided
[INFO]    |  |  |  +- commons-collections:commons-collections:jar:3.2.2:provided
[INFO]    |  |  |  +- org.mortbay.jetty:jetty-sslengine:jar:6.1.26:provided
[INFO]    |  |  |  +- javax.servlet.jsp:jsp-api:jar:2.1:provided
[INFO]    |  |  |  +- commons-configuration:commons-configuration:jar:1.6:provided
[INFO]    |  |  |  |  \- commons-digester:commons-digester:jar:1.8:provided
[INFO]    |  |  |  |     \- commons-beanutils:commons-beanutils:jar:1.7.0:provided
[INFO]    |  |  |  +- com.google.code.gson:gson:jar:2.2.4:provided
[INFO]    |  |  |  +- org.apache.hadoop:hadoop-auth:jar:2.7.4:provided
[INFO]    |  |  |  |  +- org.apache.httpcomponents:httpclient:jar:4.2.5:provided
[INFO]    |  |  |  |  |  \- org.apache.httpcomponents:httpcore:jar:4.2.4:provided
[INFO]    |  |  |  |  \- org.apache.directory.server:apacheds-kerberos-codec:jar:2.0.0-M15:provided
[INFO]    |  |  |  |     +- org.apache.directory.server:apacheds-i18n:jar:2.0.0-M15:provided
[INFO]    |  |  |  |     +- org.apache.directory.api:api-asn1-api:jar:1.0.0-M20:provided
[INFO]    |  |  |  |     \- org.apache.directory.api:api-util:jar:1.0.0-M20:provided
[INFO]    |  |  |  +- org.apache.curator:curator-client:jar:2.7.1:provided
[INFO]    |  |  |  \- org.apache.htrace:htrace-core:jar:3.1.0-incubating:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-hdfs:jar:2.7.4:provided
[INFO]    |  |  |  +- org.mortbay.jetty:jetty-util:jar:6.1.26:provided
[INFO]    |  |  |  \- xerces:xercesImpl:jar:2.9.1:provided
[INFO]    |  |  |     \- xml-apis:xml-apis:jar:1.3.04:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-mapreduce-client-app:jar:2.7.4:provided
[INFO]    |  |  |  +- org.apache.hadoop:hadoop-mapreduce-client-common:jar:2.7.4:provided
[INFO]    |  |  |  |  +- org.apache.hadoop:hadoop-yarn-client:jar:2.7.4:provided
[INFO]    |  |  |  |  \- org.apache.hadoop:hadoop-yarn-server-common:jar:2.7.4:provided
[INFO]    |  |  |  \- org.apache.hadoop:hadoop-mapreduce-client-shuffle:jar:2.7.4:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-yarn-api:jar:2.7.4:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.7.4:provided
[INFO]    |  |  |  \- org.apache.hadoop:hadoop-yarn-common:jar:2.7.4:provided
[INFO]    |  |  |     +- javax.xml.bind:jaxb-api:jar:2.2.2:provided
[INFO]    |  |  |     |  \- javax.xml.stream:stax-api:jar:1.0-2:provided
[INFO]    |  |  |     +- org.codehaus.jackson:jackson-jaxrs:jar:1.9.13:provided
[INFO]    |  |  |     \- org.codehaus.jackson:jackson-xc:jar:1.9.13:provided
[INFO]    |  |  +- org.apache.hadoop:hadoop-mapreduce-client-jobclient:jar:2.7.4:provided
[INFO]    |  |  \- org.apache.hadoop:hadoop-annotations:jar:2.7.4:provided
[INFO]    |  +- org.apache.spark:spark-launcher_2.12:jar:3.0.0:provided
[INFO]    |  +- org.apache.spark:spark-kvstore_2.12:jar:3.0.0:provided
[INFO]    |  |  \- org.fusesource.leveldbjni:leveldbjni-all:jar:1.8:provided
[INFO]    |  +- org.apache.spark:spark-network-common_2.12:jar:3.0.0:provided
[INFO]    |  +- org.apache.spark:spark-network-shuffle_2.12:jar:3.0.0:provided
[INFO]    |  +- org.apache.spark:spark-unsafe_2.12:jar:3.0.0:provided
[INFO]    |  +- javax.activation:activation:jar:1.1.1:provided
[INFO]    |  +- org.apache.curator:curator-recipes:jar:2.7.1:provided
[INFO]    |  |  +- org.apache.curator:curator-framework:jar:2.7.1:provided
[INFO]    |  |  \- com.google.guava:guava:jar:16.0.1:provided
[INFO]    |  +- org.apache.zookeeper:zookeeper:jar:3.4.14:provided
[INFO]    |  |  \- org.apache.yetus:audience-annotations:jar:0.5.0:provided
[INFO]    |  +- javax.servlet:javax.servlet-api:jar:3.1.0:provided
[INFO]    |  +- org.apache.commons:commons-lang3:jar:3.9:provided
[INFO]    |  +- org.apache.commons:commons-math3:jar:3.4.1:provided
[INFO]    |  +- org.apache.commons:commons-text:jar:1.6:provided
[INFO]    |  +- com.google.code.findbugs:jsr305:jar:3.0.0:provided
[INFO]    |  +- org.slf4j:jul-to-slf4j:jar:1.7.30:provided
[INFO]    |  +- org.slf4j:jcl-over-slf4j:jar:1.7.30:provided
[INFO]    |  +- log4j:log4j:jar:1.2.17:provided
[INFO]    |  +- org.slf4j:slf4j-log4j12:jar:1.7.30:provided
[INFO]    |  +- com.ning:compress-lzf:jar:1.0.3:provided
[INFO]    |  +- org.xerial.snappy:snappy-java:jar:1.1.7.5:compile
[INFO]    |  +- org.lz4:lz4-java:jar:1.7.1:compile
[INFO]    |  +- com.github.luben:zstd-jni:jar:1.4.4-3:compile
[INFO]    |  +- org.roaringbitmap:RoaringBitmap:jar:0.7.45:provided
[INFO]    |  |  \- org.roaringbitmap:shims:jar:0.7.45:provided
[INFO]    |  +- commons-net:commons-net:jar:3.1:provided
[INFO]    |  +- org.scala-lang.modules:scala-xml_2.12:jar:1.2.0:provided
[INFO]    |  +- org.scala-lang:scala-library:jar:2.12.10:compile
[INFO]    |  +- org.scala-lang:scala-reflect:jar:2.12.10:provided
[INFO]    |  +- org.json4s:json4s-jackson_2.12:jar:3.6.6:provided
[INFO]    |  |  \- org.json4s:json4s-core_2.12:jar:3.6.6:provided
[INFO]    |  |     +- org.json4s:json4s-ast_2.12:jar:3.6.6:provided
[INFO]    |  |     \- org.json4s:json4s-scalap_2.12:jar:3.6.6:provided
[INFO]    |  +- org.glassfish.jersey.core:jersey-client:jar:2.30:provided
[INFO]    |  +- org.glassfish.jersey.core:jersey-server:jar:2.30:provided
[INFO]    |  |  +- org.glassfish.jersey.media:jersey-media-jaxb:jar:2.30:provided
[INFO]    |  |  \- jakarta.validation:jakarta.validation-api:jar:2.0.2:provided
[INFO]    |  +- org.glassfish.jersey.containers:jersey-container-servlet:jar:2.30:provided
[INFO]    |  +- org.glassfish.jersey.containers:jersey-container-servlet-core:jar:2.30:provided
[INFO]    |  +- org.glassfish.jersey.inject:jersey-hk2:jar:2.30:provided
[INFO]    |  |  +- org.glassfish.hk2:hk2-locator:jar:2.6.1:provided
[INFO]    |  |  |  +- org.glassfish.hk2.external:aopalliance-repackaged:jar:2.6.1:provided
[INFO]    |  |  |  +- org.glassfish.hk2:hk2-api:jar:2.6.1:provided
[INFO]    |  |  |  \- org.glassfish.hk2:hk2-utils:jar:2.6.1:provided
[INFO]    |  |  \- org.javassist:javassist:jar:3.25.0-GA:provided
[INFO]    |  +- io.netty:netty-all:jar:4.1.47.Final:provided
[INFO]    |  +- com.clearspring.analytics:stream:jar:2.9.6:provided
[INFO]    |  +- io.dropwizard.metrics:metrics-core:jar:4.1.1:provided
[INFO]    |  +- io.dropwizard.metrics:metrics-jvm:jar:4.1.1:provided
[INFO]    |  +- io.dropwizard.metrics:metrics-json:jar:4.1.1:provided
[INFO]    |  +- io.dropwizard.metrics:metrics-graphite:jar:4.1.1:provided
[INFO]    |  +- io.dropwizard.metrics:metrics-jmx:jar:4.1.1:provided
[INFO]    |  +- com.fasterxml.jackson.module:jackson-module-scala_2.12:jar:2.10.0:provided
[INFO]    |  |  \- com.fasterxml.jackson.module:jackson-module-paranamer:jar:2.10.0:provided
[INFO]    |  +- org.apache.ivy:ivy:jar:2.4.0:provided
[INFO]    |  +- oro:oro:jar:2.0.8:provided
[INFO]    |  +- net.razorvine:pyrolite:jar:4.30:provided
[INFO]    |  +- net.sf.py4j:py4j:jar:0.10.9:provided
[INFO]    |  \- org.apache.commons:commons-crypto:jar:1.0.0:provided
[INFO]    +- org.apache.spark:spark-catalyst_2.12:jar:3.0.0:provided
[INFO]    |  +- org.scala-lang.modules:scala-parser-combinators_2.12:jar:1.1.2:provided
[INFO]    |  +- org.codehaus.janino:janino:jar:3.0.16:provided
[INFO]    |  +- org.codehaus.janino:commons-compiler:jar:3.0.16:provided
[INFO]    |  +- org.antlr:antlr4-runtime:jar:4.7.1:provided
[INFO]    |  +- commons-codec:commons-codec:jar:1.10:provided
[INFO]    |  \- org.apache.arrow:arrow-vector:jar:0.15.1:provided
[INFO]    |     +- org.apache.arrow:arrow-format:jar:0.15.1:provided
[INFO]    |     +- org.apache.arrow:arrow-memory:jar:0.15.1:provided
[INFO]    |     \- com.google.flatbuffers:flatbuffers-java:jar:1.9.0:provided
[INFO]    +- org.apache.spark:spark-tags_2.12:jar:3.0.0:compile
[INFO]    +- org.apache.orc:orc-core:jar:1.5.10:provided
[INFO]    |  +- org.apache.orc:orc-shims:jar:1.5.10:provided
[INFO]    |  +- com.google.protobuf:protobuf-java:jar:2.5.0:provided
[INFO]    |  +- commons-lang:commons-lang:jar:2.6:provided
[INFO]    |  +- io.airlift:aircompressor:jar:0.10:provided
[INFO]    |  \- org.threeten:threeten-extra:jar:1.5.0:provided
[INFO]    +- org.apache.orc:orc-mapreduce:jar:1.5.10:provided
[INFO]    +- org.apache.hive:hive-storage-api:jar:2.7.1:provided
[INFO]    +- org.apache.parquet:parquet-column:jar:1.10.1:provided
[INFO]    |  +- org.apache.parquet:parquet-common:jar:1.10.1:provided
[INFO]    |  \- org.apache.parquet:parquet-encoding:jar:1.10.1:provided
[INFO]    +- org.apache.parquet:parquet-hadoop:jar:1.10.1:provided
[INFO]    |  +- org.apache.parquet:parquet-format:jar:2.4.0:provided
[INFO]    |  +- org.apache.parquet:parquet-jackson:jar:1.10.1:provided
[INFO]    |  +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13:provided
[INFO]    |  \- org.codehaus.jackson:jackson-core-asl:jar:1.9.13:provided
[INFO]    +- com.fasterxml.jackson.core:jackson-databind:jar:2.10.0:compile
[INFO]    |  \- com.fasterxml.jackson.core:jackson-annotations:jar:2.10.0:compile
[INFO]    +- org.apache.xbean:xbean-asm7-shaded:jar:4.15:provided
[INFO]    \- org.spark-project.spark:unused:jar:1.0.0:compile
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

#### Running the JAR without dependencies

Ref https://github.com/AbsaOSS/ABRiS/issues/165#issuecomment-722987624 , we ran the app using the JAR without dependencies, using the command 
```shell script
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop3.2.1:latest spark-submit --repositories https://packages.confluent.io/maven --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,za.co.absa:abris_2.12:4.0.0 --deploy-mode client --class org.example.App target/simplespark-1.0-SNAPSHOT.jar
```

It, however, failed to load the abris 4.0.0 package using Spark-submit, with the following error
```text
:::: WARNINGS
                [NOT FOUND  ] javax.ws.rs#javax.ws.rs-api;2.1.1!javax.ws.rs-api.${packaging.type} (0ms)

        ==== central: tried

          https://repo1.maven.org/maven2/javax/ws/rs/javax.ws.rs-api/2.1.1/javax.ws.rs-api-2.1.1.${packaging.type}

                ::::::::::::::::::::::::::::::::::::::::::::::

                ::              FAILED DOWNLOADS            ::

                :: ^ see resolution messages for details  ^ ::

                ::::::::::::::::::::::::::::::::::::::::::::::

                :: javax.ws.rs#javax.ws.rs-api;2.1.1!javax.ws.rs-api.${packaging.type}

                ::::::::::::::::::::::::::::::::::::::::::::::



:: USE VERBOSE OR DEBUG MESSAGE LEVEL FOR MORE DETAILS
Exception in thread "main" java.lang.RuntimeException: [download failed: javax.ws.rs#javax.ws.rs-api;2.1.1!javax.ws.rs-api.${packaging.type}]
        at org.apache.spark.deploy.SparkSubmitUtils$.resolveMavenCoordinates(SparkSubmit.scala:1389)
        at org.apache.spark.deploy.DependencyUtils$.resolveMavenDependencies(DependencyUtils.scala:54)
        at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:308)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:871)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1007)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1016)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
```

We get the same error when we try to load spark-shell using the command
```shell script
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop3.2.1:latest spark-shell --repositories https://packages.confluent.io/maven --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,za.co.absa:abris_2.12:4.0.0
```

However, if we use Abris 3.2.2 instead of 4.0.0, spark shell loads fine with the command
```shell script
docker run -v $(pwd):/core -w /core -it --rm --network docker_kafka_server_default  spark3.0.1-scala2.12-hadoop3.2.1:latest spark-shell --repositories https://packages.confluent.io/maven --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,za.co.absa:abris_2.12:3.2.2
```

### Clean up

- Clean up dockerised kafka cluster by running 
```shell script
docker-compose -f docker_kafka_server/docker-compose.yml down
```