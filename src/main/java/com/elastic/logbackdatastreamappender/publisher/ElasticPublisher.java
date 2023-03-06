package com.elastic.logbackdatastreamappender.publisher;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.PatternLayoutBase;
import com.elastic.logbackdatastreamappender.utils.ElasticWriter;
import com.elastic.logbackdatastreamappender.utils.Settings;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticPublisher implements Runnable {

    private Settings settings;
    private volatile List<ILoggingEvent> events;
    private JsonFactory jsonFactory;
    private JsonGenerator jsonGenerator;

    private AtomicInteger threadNumber = new AtomicInteger(0);

    private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        }
    };

    public ElasticPublisher(Settings settings, Context context) throws IOException {
        PatternLayoutBase<ILoggingEvent> layout = new PatternLayout();
        layout.setContext(context);
        layout.setPattern("%thread");
        this.settings = settings;

        jsonFactory = new JsonFactory();
        events = new ArrayList<>();

      jsonGenerator = jsonFactory.createGenerator(new ElasticWriter(settings));
    }

    public synchronized void addEvent(ILoggingEvent ILoggingEvent) {
        events.add(ILoggingEvent);

        Thread thread = new Thread(this, "write from thread : " + threadNumber.getAndIncrement());


        thread.start();

    }

    @Override
    public void run() {


        interceptEventLoggein();
//        interceptEventLoggeing();
    }
    public synchronized void interceptEventLoggein() {
//        events.forEach(x -> System.out.println(
//                "Current Thread ID: "
//                        + Thread.currentThread().getName()  + " from run " + x.getFormattedMessage() ));
        interceptEventLoggeing();
        events = new ArrayList<>();
    }
    public synchronized void interceptEventLoggeing() {
        try {
            Thread.sleep(settings.getSleepTime());
            for (ILoggingEvent event : events) {

                generateJson(event);


            }
            events = new ArrayList<>();

            jsonGenerator.flush();

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void generateJson(ILoggingEvent event) throws IOException {


        jsonGenerator.writeStartObject();
        jsonGenerator.writeFieldName("create");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeEndObject();
        jsonGenerator.writeEndObject();
        jsonGenerator.writeRaw('\n');
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField("@timestamp", DATE_FORMAT.get().format(new Date(event.getTimeStamp())));
        jsonGenerator.writeObjectField("message", event.getFormattedMessage());
        for (Map.Entry<String, String> entry : event.getMDCPropertyMap().entrySet()) {
            jsonGenerator.writeObjectField(entry.getKey(), entry.getValue());
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.writeRaw('\n');

    }
}
