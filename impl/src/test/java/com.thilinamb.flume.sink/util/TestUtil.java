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
    private ZooKeeperLocal zookeeper;
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

    private void shutdownKafkaServer(){
        kafkaServer.stop();
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
