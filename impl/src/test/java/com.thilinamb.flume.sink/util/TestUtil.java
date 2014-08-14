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

import com.thilinamb.flume.sink.util.consumer.KafkaConsumer;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
    private static TestUtil instance = new TestUtil();

    private KafkaLocal kafkaServer;
    private KafkaConsumer kafkaConsumer;

    private TestUtil(){
    }

    public static TestUtil getInstance(){
        return instance;
    }

    private boolean startKafkaServer() {
        Properties kafkaProperties = new Properties();
        Properties zkProperties = new Properties();

        try {
            //load properties
            kafkaProperties.load(Class.class.getResourceAsStream("/kafka-server.properties"));
            zkProperties.load(Class.class.getResourceAsStream("/zookeeper.properties"));

            //start local zookeeper
            ZooKeeperLocal zookeeper = new ZooKeeperLocal(zkProperties);
            logger.info("ZooKeeper instance is started.");
            //start kafkaServer
            kafkaServer = new KafkaLocal(kafkaProperties);
            kafkaServer.start();
            logger.info("Kafka Server was started successfully!");
            return true;

        } catch (Exception e){
            logger.error("Error starting the Kafka Server.", e);
            return false;
        }
    }

    private KafkaConsumer getKafkaConsumer(){
        synchronized (this){
            if(kafkaConsumer == null){
                kafkaConsumer = new KafkaConsumer();
            }
        }
        return kafkaConsumer;
    }

    public void initTopicList(List<String> topics){
        getKafkaConsumer().initTopicList(topics);
    }

    public MessageAndMetadata getNextMessageFromConsumer(String topic){
        return getKafkaConsumer().getNextMessage(topic);
    }

    public void prepare(){
        boolean startStatus = startKafkaServer();
        if(!startStatus){
            throw new RuntimeException("Error starting the server!");
        }
        try {
            Thread.sleep(3*1000);   // add this sleep time to
            // ensure that the server is fully started before proceeding with tests.
        } catch (InterruptedException e) {
            // ignore
        }
        getKafkaConsumer();
        logger.info("Completed the prepare phase.");
    }

    public void tearDown(){
        logger.info("Shutting down the Kafka Consumer.");
        getKafkaConsumer().shutdown();
        try {
            Thread.sleep(3*1000);   // add this sleep time to
            // ensure that the server is fully started before proceeding with tests.
        } catch (InterruptedException e) {
            // ignore
        }
        logger.info("Shutting down the kafka Server.");
        kafkaServer.stop();
        logger.info("Completed the tearDown phase.");
    }
}
