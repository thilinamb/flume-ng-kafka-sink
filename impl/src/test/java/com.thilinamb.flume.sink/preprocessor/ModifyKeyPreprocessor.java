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
