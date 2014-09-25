#  Flume-NG-Kafka-Sink

This is a [Flume](https://flume.apache.org) Sink implementation that can publish data to a [Kafka](http://kafka.apache.org) topic.
The objective is to integrate Flume with Kafka so that pull based processing systems such as [Apache Storm](https://storm.incubator.apache.org) can process the data coming through various Flume sources such as Syslog.

### This is now a part of the official Flume distribution (from v1.6 onwards) along with significant improvements.

## Updates
- Aug 23, 2014 - This implementation is submitted as a new feature to Apache Flume. Associated Jira for this feature - [https://issues.apache.org/jira/browse/FLUME-2251](https://issues.apache.org/jira/browse/FLUME-2251) 
- Aug 17, 2014 - Unit tests are added and linked with the build.

## Example Usecase
Realtime Syslog processing architecture using Apache Flume, Apache Kafka and Apache Storm.
![Realtime Syslog Processing](/realtime-syslog-processing-archi.png)

## Dependency Versions
- Apache Flume - 1.5.0
- Apache Kafka - 0.8.1.1

## Prerequisites
- Java 1.6 or higher
- [Apache Maven 3](http://maven.apache.org)
- An [Apache Flume](https://flume.apache.org) installation (See the dependent version above)
- An [Apache Kafka](http://kafka.apache.org) installation (See the dependent version above)

## Building the project
[Apache Maven](http://maven.apache.org) is used to build the project. This [page](http://maven.apache.org/download.cgi) contains the download links and an installation guide for various operating systems.

Issue the command: > mvn clean install

This will compile the project and the binary distribution(flume-kafka-sink-dist-x.x.x-bin.zip) will be copied into '${project_root}/dist/target' directory.

## Setting up

1. Build the project as per the instructions in the previous subsection.
2. Unzip the binary distribution(flume-kafka-sink-dist-x.x.x-bin.zip) inside ${project_root}/dist/target.
3. There are two ways to include this custom sink in Flume binary installation.

_Recommended Approach_
- Create a new directory inside `plugins.d` directory which is located in `${FLUME_HOME}`. If the `plugins.d` directory is not there, go ahead and create it. We will call this new directory that was created inside plugins.d 'kafka-sink'. You can give it any name depending on the naming conventions you prefer.
- Inside this new directory (kafka-sink) create two subdirectories called `lib` and `libext`.
- You can find the jar files for this sink inside the `lib` directory of the extracted archive. Copy `flume-kafka-sink-impl-x.x.x.jar` into the `plugins.d/kafka-sink/lib` directory. Then copy the rest of the jars into the `plugins.d/kafka-sink/libext` directory.

This is how it'll look like at the end.
```
${FLUME_HOME}
 |-- plugins.d
 		|-- kafka-sink
 			|-- lib
   				|-- flume-kafka-sink-impl-x.x.x.jar
 			|-- libext
   				|-- kafka_x.x.-x.x.x.x.jar
   				|-- metrics-core-x.x.x.jar
   				|-- scala-library-x.x.x.jar
```
More details can be found in the [Flume user guide](https://flume.apache.org/FlumeUserGuide.html#the-plugins-d-directory).

**OR**
                                
_Quick and Dirty Approach_
- Copy the jar files inside the `lib` directory of extracted archive into `${FLUME_HOME}/lib`.

## Configuration
Following parameters are supported at the moment.

- **type**
	- The sink type. This should be set as `com.thilinamb.flume.sink.KafkaSink`.

- **topic**[optional] 
	- The topic in Kafka to which the messages will be published. If this topic is mentioned, every message will be published to the same topic. If dynamic topics are required, it's possible to use a preprocessor instead of a static topic. It's mandatory that either of the parameters _topic_ or _preprocessor_ is provided, because the topic cannot be null when publishing to Kafka. If none of these parameters are provided, the messages will be published to a default topic called `default-flume-topic`.

- **preprocessor**[optional]
	- This is an extension point provided support dynamic topics and keys. Also it's possible to use it to support message modification before publishing to Kafka. The full qualified class name of the preprocessor implementation should be provided here. Refer the next subsection to read more about preprocessors. If a preprocessor is not configured, then a static topic should be used as explained before. And the messages will not be keyed. In a primitive setup, configuring a static topic would suffice.

- **Kafka Producer Properties**
	- These properties are used to configure the Kafka Producer. Any producer property supported by Kafka can be used. The only requirement is to prepend the property name with the prefix `kafka.`. For instance, the `metadata.broker.list` property should be written as `kafka.metadata.broker.list`. Please take a look at the [sample configuration](https://github.com/thilinamb/flume-kafka-sink/blob/master/conf/flume-kafka.conf) provided in the `conf` directory of the distribution.
    
## Implementing a preprocessor
Implementing a custom preprocessor is useful to support dynamic topics and keys. Also they support message transformations. The requirement is to implement the interface `com.thilinamb.flume.sink.MessagePreprocessor`. The java-docs of this interface provides a detailed description of the methods, parameters, etc. There are three methods that needs to be implemented. The method names are self explainatory.

- ```public String extractKey(Event event, Context context)```
- ```public String extractTopic(Event event, Context context)```
- ```public String transformMessage(Event event, Context context)```

The class '[com.thilinamb.flume.sink.example.SimpleMessagePreprocessor](https://github.com/thilinamb/flume-kafka-sink/blob/master/example/src/main/java/com/thilinamb/flume/sink/example/SimpleMessagePreprocessor.java)' inside the 'example' module is an example implementation of a preprocessor.

After implementing the preprocessor, compile it into a jar and add into the Flume classpath with the rest of the jars (copy to `libext` if you are using the `plugins.d` directory or copy it to `${FLUME_HOME}\lib` if you are using the other approach) and configure the `preprocessor` parameter with its fully qualified classname. For instance;

`a1.sinks.k1.preprocessor = com.thilinamb.flume.sink.example.SimpleMessagePreprocessor`

## Questions and Feedback
Please file a bug or contact me via [email](mailto:thilinamb@gmail.com) with respect to any bug you encounter or any other feedback.








