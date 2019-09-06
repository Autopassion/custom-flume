package com.bigfang.sources;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data
            Event e = new SimpleEvent();
            //自定义数据写入
            for (int i = 0;i<=5;i++) {
                String data = i+"-eventData";
                e.setBody(data.getBytes());
            }
            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);
            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            getChannelProcessor().close();
        }
        return status;
    }

    //设置source的配置信息
    @Override
    public void configure(Context context) {
        String prefix = context.getString("prefix", ".testData");
        this.prefix = prefix;
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}
