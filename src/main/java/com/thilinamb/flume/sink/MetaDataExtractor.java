/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package com.thilinamb.flume.sink;

import org.apache.flume.Context;

/**
 * This class provides the ability to use a custom key/topic extraction mechanism
 * When publishing to Kafka. Kafka uses the key to partition the messages.
 * If an implementation of this class is not provided, the message will be
 * published to a random partition.
 * Providing a topic to publish is mandatory. Alternatively it's possible to provide
 * the topic through the Flume configuration file using the property "topic".
 * The user can extend this class and implement
 * the <code>extractKey</code> method by including the logic to extract
 * a key based on the message and the Flume context.
 * For instance, in a Syslog record processing scenario the date or hour extracted
 * from the timestamp can be used as the key.
 * Or it's possible to include the partition data in the properties file and read
 * from the Flume Context.
 * Also make sure to keep the default constructor in the implementation, because
 * it is used to instantiate through reflection.
 * The implementation should be compiled and included in the Flume classpath
 * when starting Flume. And the property "metadata.extractor" should be set in
 * the Flume configuration file.
 */
public interface MetaDataExtractor {

    /**
     * Extract a key from the message and/or Flume runtime.
     * @param message This is the message that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return Key extracted based on the implemented logic
     */
    public String extractKey(String message, Context context);

    /**
     * Extract a topic for the message
     * @param message This is the message that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return topic extracted based on the implemented logic
     */
    public String extractTopic(String message, Context context);
}
