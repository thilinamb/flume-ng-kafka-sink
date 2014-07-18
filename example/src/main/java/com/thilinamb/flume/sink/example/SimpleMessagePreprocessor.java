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

package com.thilinamb.flume.sink.example;

import com.thilinamb.flume.sink.MessagePreprocessor;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * This is an example of a <code>MessagePreprocessor</code> implementation.
 */
public class SimpleMessagePreprocessor implements MessagePreprocessor {

    /**
     * extract the hour of the time stamp as the key. So the data is partitioned
     * per hour.
     * @param event This is the Flume event that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return Hour of the timestamp
     */
    @Override
    public String extractKey(Event event, Context context) {
        // get timestamp header if it's present.
        String timestampStr = event.getHeaders().get("timestamp");
        if(timestampStr != null){
            // parse it and get the hour
            Long timestamp = Long.parseLong(timestampStr);
            Calendar cal = Calendar.getInstance();
            cal.setTimeZone(TimeZone.getTimeZone("UTC"));
            cal.setTimeInMillis(timestamp);
            return Integer.toString(cal.get(Calendar.HOUR_OF_DAY));
        }
        return null;    // return null otherwise
    }

    /**
     * A custom property is read from the Flume config.
     * @param event This is the Flume event that will be sent to Kafka
     * @param context The Flume runtime context.
     * @return topic provided as a custom property
     */
    @Override
    public String extractTopic(Event event, Context context) {
        return context.getString("custom-topic", "default-topic");
    }

    /**
     * Trying to prepend each message with the timestamp.
     * @param event Flume event received by the sink.
     * @param context Flume context
     * @return modified message of the form: timestamp + ":" + original message body
     */
    @Override
    public String transformMessage(Event event, Context context) {
        String messageBody = new String(event.getBody());
        String timestampStr = event.getHeaders().get("timestamp");
        if(timestampStr != null){
            messageBody = timestampStr.concat(": " + messageBody);
        }
        return messageBody;
    }
}
