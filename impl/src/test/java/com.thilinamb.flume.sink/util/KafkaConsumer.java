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

package com.thilinamb.flume.sink.util;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Kafka Consumer implementation. This uses the current thread to fetch the
 * next message from the queue and doesn't use a multi threaded implementation.
 * So this implements a synchronous blocking call.
 * To avoid infinite waiting, a timeout is implemented to wait only for 10 seconds
 * before concluding that the message will not be available.
 */
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ConsumerConnector consumer;
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;

    public KafkaConsumer() {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(TestUtil.getInstance().getZkUrl(), "group_1"));
    }

    private static ConsumerConfig createConsumerConfig(String zkUrl, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkUrl);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    public void initTopicList(List<String> topics){
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        for (String topic : topics) {
            topicCountMap.put(topic, new Integer(1)); // we need only single threaded consumers
        }
        consumerMap = consumer.createMessageStreams(topicCountMap);
    }

    public MessageAndMetadata getNextMessage(String topic){
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        KafkaStream stream = streams.get(0); // it has only a single stream, because there is only one consumer
        final ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int counter = 0;
        while (!it.hasNext()){
            // Wait time >= 10s, so return null and exit
            if(counter == 5){
                logger.error("0 messages available to fetch for the topic " + topic);
                return null;
            }
            // wait till a message is published. this is a blocking call.
            try {
                Thread.sleep(2 * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
            counter++;
        }
        return it.next();
    }

    public void shutdown(){
        consumer.shutdown();
    }
}
