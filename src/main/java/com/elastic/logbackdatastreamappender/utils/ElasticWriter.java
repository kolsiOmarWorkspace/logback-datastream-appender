package com.elastic.logbackdatastreamappender.utils;



import java.io.*;

import static com.elastic.logbackdatastreamappender.utils.AppendersUtils.doPost;

public class ElasticWriter extends Writer {

    private Settings settings;
    StringBuilder stringBuilder;

    public ElasticWriter(Settings settings) {
        stringBuilder = new StringBuilder();
        this.settings = settings;
    }

    @Override
    public void write(char[] chars, int i, int i1) throws IOException {
        stringBuilder.append(chars, i, i1);
//        System.out.println(Thread.currentThread().getName() + " from elasticWriter : " + stringBuilder.toString());
    }

    @Override
    public void flush() throws IOException {
//        comparing spring buffer length with the queue size
        if (this.settings.getMaxQueueSize() >= stringBuilder.length()) {
//        send to elastic ...

            doPost(this.settings.getUrl().toString() + settings.getIndex() +"/_bulk"   , stringBuilder.toString(), Methods.PUT);
            stringBuilder = new StringBuilder();
        } else {
            stringBuilder = new StringBuilder();
            throw new RuntimeException("event size is bigger than the Queue size");
        }

    }

    @Override
    public void close() throws IOException {
    }



}

