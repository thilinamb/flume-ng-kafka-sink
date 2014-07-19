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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and a channel.
 * This supports key and messages of type String.
 * Extension points are provided to for users to implement custom key and topic extraction
 * logic based on the message content as well as the Flume context.
 * Without implementing this extension point(MessagePreprocessor), it's possible to publish
 * messages based on a static topic. In this case messages will be published to a random
 * partition.
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private Properties producerProps;
    private Producer<String, String> producer;
    private MessagePreprocessor messagePreProcessor;
    private String topic;
    private Context context;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        String eventTopic = topic;
        String eventKey = null;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
                // get the message body.
                String eventBody = new String(event.getBody());
                // if the metadata extractor is set, extract the topic and the key.
                if (messagePreProcessor != null) {
                    eventBody = messagePreProcessor.transformMessage(event, context);
                    eventTopic = messagePreProcessor.extractTopic(event, context);
                    eventKey = messagePreProcessor.extractKey(event, context);
                }
                // log the event for debugging
                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventBody);
                }
                // create a message
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(eventTopic, eventKey,
                        eventBody);
                // publish
                producer.send(data);
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            // publishing is successful. Commit.
            transaction.commit();

        } catch (Exception ex) {
            transaction.rollback();
            String errorMsg = "Failed to publish event: " + event;
            logger.error(errorMsg);
            throw new EventDeliveryException(errorMsg, ex);

        } finally {
            transaction.close();
        }

        return result;
    }

    @Override
    public synchronized void start() {
        // instantiate the producer
        ProducerConfig config = new ProducerConfig(producerProps);
        producer = new Producer<String, String>(config);
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }


    @Override
    public void configure(Context context) {
        this.context = context;
        // read the properties for Kafka Producer
        // any property that has the prefix "kafka" in the key will be considered as a property that is passed when
        // instantiating the producer.
        // For example, kafka.metadata.broker.list = localhost:9092 is a property that is processed here, but not
        // sinks.k1.type = com.thilinamb.flume.sink.KafkaSink.
        Map<String, String> params = context.getParameters();
        producerProps = new Properties();
        for (String key : params.keySet()) {
            String value = params.get(key).trim();
            key = key.trim();
            if (key.startsWith(Constants.PROPERTY_PREFIX)) {
                // remove the prefix
                key = key.substring(Constants.PROPERTY_PREFIX.length() + 1, key.length());
                producerProps.put(key.trim(), value);
                if (logger.isDebugEnabled()) {
                    logger.debug("Reading a Kafka Producer Property: key: " + key + ", value: " + value);
                }
            }
        }

        // get the message Preprocessor if set
        String preprocessorClassName = context.getString(Constants.PREPROCESSOR);
        // if it's set create an instance using Java Reflection.
        if (preprocessorClassName != null) {
            try {
                Class preprocessorClazz = Class.forName(preprocessorClassName.trim());
                Object preprocessorObj = preprocessorClazz.newInstance();
                if (preprocessorObj instanceof MessagePreprocessor) {
                    messagePreProcessor = (MessagePreprocessor) preprocessorObj;
                } else {
                    String errorMsg = "Provided class for MessagePreprocessor does not implement " +
                            "'com.thilinamb.flume.sink.MessagePreprocessor'";
                    logger.error(errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }
            } catch (ClassNotFoundException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (InstantiationException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            } catch (IllegalAccessException e) {
                String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
                logger.error(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            }
        }

        if (messagePreProcessor == null) {
            // MessagePreprocessor is not set. So read the topic from the config.
            topic = context.getString(Constants.TOPIC, Constants.DEFAULT_TOPIC);
            if (topic.equals(Constants.DEFAULT_TOPIC)) {
                logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name" +
                        Constants.DEFAULT_TOPIC);
            } else {
                logger.info("Using the static topic: " + topic);
            }
        }
    }
}
