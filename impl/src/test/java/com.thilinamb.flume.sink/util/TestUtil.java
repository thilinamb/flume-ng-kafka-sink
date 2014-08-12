package com.thilinamb.flume.sink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);

    private KafkaLocal kafkaServer;
    private ZooKeeperLocal zookeeper;

    public boolean startKafkaServer() {
        Properties kafkaProperties = new Properties();
        Properties zkProperties = new Properties();

        try {
            //load properties
            kafkaProperties.load(Class.class.getResourceAsStream("/kafka-server.properties"));
            zkProperties.load(Class.class.getResourceAsStream("/zookeeper.properties"));

            //start local zookeeper
            zookeeper = new ZooKeeperLocal(zkProperties);
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

    public void shutdownKafkaServer(){
        kafkaServer.stop();
    }

}
