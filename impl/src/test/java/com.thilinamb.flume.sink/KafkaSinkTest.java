package com.thilinamb.flume.sink;

import com.thilinamb.flume.sink.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for Kafka Sink
 */
public class KafkaSinkTest {

    private TestUtil testUtil = new TestUtil();;

    @Before
    public void setup(){
        testUtil.startKafkaServer();
    }

    @Test
    public void testKafkaServerStart(){
        // Temp test to check the setup.
        assertEquals(1,1);
    }

    @After
    public void tearDown(){
        testUtil.shutdownKafkaServer();
    }

}
