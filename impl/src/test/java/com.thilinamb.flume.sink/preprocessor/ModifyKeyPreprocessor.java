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

package com.thilinamb.flume.sink.preprocessor;

import com.thilinamb.flume.sink.Constants;
import com.thilinamb.flume.sink.MessagePreprocessor;
import com.thilinamb.flume.sink.TestConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * Implementation of <code>com.thilinamb.flume.sink.MessagePreprocessor</code>
 * for unit tests.
 * Only modify the key without altering topic or message body.
 */
public class ModifyKeyPreprocessor implements MessagePreprocessor {
    @Override
    public String extractKey(Event event, Context context) {
        return TestConstants.CUSTOM_KEY;
    }

    @Override
    public String extractTopic(Event event, Context context) {
        return context.getString(Constants.TOPIC);
    }

    @Override
    public String transformMessage(Event event, Context context) {
        return new String(event.getBody());
    }
}
