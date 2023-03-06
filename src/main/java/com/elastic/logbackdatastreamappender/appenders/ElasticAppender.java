package com.elastic.logbackdatastreamappender.appenders;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.elastic.logbackdatastreamappender.utils.ElasticsearchProperties;
import com.elastic.logbackdatastreamappender.publisher.ElasticPublisher;
import com.elastic.logbackdatastreamappender.utils.Methods;
import com.elastic.logbackdatastreamappender.utils.Settings;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.web.WebApplicationInitializer;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.elastic.logbackdatastreamappender.utils.AppendersUtils.doPost;

@Component
@PropertySource("classpath:application.properties")
public class ElasticAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements WebApplicationInitializer {

    public boolean allowDataStream;

    public Settings settings;

    public ElasticAppender() {
        settings = new Settings();
    }

    public ElasticAppender(Settings settings) {
        settings.toString();
    }




    public void setIndex(String index) {
        this.settings.setIndex(index);
    }

    public boolean isAllowDataStream() {
        return allowDataStream;
    }

    public void setType(String type) {
        this.settings.setType(type);
    }


    public void setSleepTime(int sleepTime) {
        if (sleepTime < 100) {
            sleepTime = 100;
        }
        this.settings.setSleepTime(sleepTime);
    }


    public void setMaxRetries(int maxRetries) {
        this.settings.setMaxRetries(maxRetries);
    }


    public void setConnectTimeout(int connectTimeout) {
        this.settings.setConnectTimeout(connectTimeout);
    }

    public void setReadTimeout(int readTimeout) {
        this.settings.setReadTimeout(readTimeout);
    }


    public void setLogsToStderr(boolean logsToStderr) {
        this.settings.setLogsToStderr(logsToStderr);
    }


    public void setErrorsToStderr(boolean errorsToStderr) {
        this.settings.setErrorsToStderr(errorsToStderr);
    }


    public void setAllowDataStream(boolean allowDataStream) {
        this.allowDataStream = allowDataStream;
    }

    public void setIncludeCallerData(boolean includeCallerData) {
        this.settings.setIncludeCallerData(includeCallerData);
    }


    public void setMaxQueueSize(int maxQueueSize) {
        this.settings.setMaxQueueSize(maxQueueSize);
    }


    public void setLoggerName(String loggerName) {
        this.settings.setLoggerName(loggerName);
    }


    public void setUrl(String url) throws MalformedURLException {
        this.settings.setUrl(new URL(url));
    }


    public void setErrorLoggerName(String errorLoggerName) {
        this.settings.setErrorLoggerName(errorLoggerName);
    }


    public void setRawJsonMessage(boolean rawJsonMessage) {
        this.settings.setRawJsonMessage(rawJsonMessage);
    }

    public void setIncludeMdc(boolean includeMdc) {
        this.settings.setIncludeMdc(includeMdc);
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.settings.setMaxMessageSize(maxMessageSize);
    }

    public void setProperties(ElasticsearchProperties elasticsearchProperties) {
        this.settings.setElasticsearchProperties(elasticsearchProperties);
    }

    ElasticPublisher elasticPublisher;

    @Override
    public void start() {
        try {

            if (allowDataStream) {


//                create index template if needed
                doPost(settings.getUrl() + "_component_template/" + settings.getIndex() + "-settings-component-template", getAndReplaceFileContent("src/main/resources/default_resources/settings-component-template.json"), Methods.PUT);
//                check if it has ilm
                doPost(settings.getUrl()+ "_component_template/" + settings.getIndex() + "-mapping-component-template", getAndReplaceFileContent("src/main/resources/default_resources/mapping-component-template.json"), Methods.PUT);
//                create ilm if needed
                doPost(settings.getUrl() + "_index_template/" + settings.getIndex() + "-template", getAndReplaceFileContent("src/main/resources/default_resources/default-index-template.json"), Methods.PUT);

                doPost(settings.getUrl() + "_ilm/policy/" + settings.getIndex() + "policy", getAndReplaceFileContent("src/main/resources/default_resources/default-lifecycle-policy.json"), Methods.PUT);
            }
//            pass settings from new constructor parametrized with settings
            elasticPublisher = new ElasticPublisher(this.settings, getContext());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.start();
    }

    private String getAndReplaceFileContent(String path) {
        Path filePath = Path.of(path);
        String fileContent = "";

        try
        {
            byte[] bytes = Files.readAllBytes(Paths.get(filePath.toUri()));
            fileContent = new String (bytes);
            fileContent = fileContent.replaceAll("\\$\\{index_name}", settings.getIndex());
            return fileContent;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void append(ILoggingEvent eventObject) {

        this.sendToPublisher(eventObject);
    }

    private void sendToPublisher(ILoggingEvent eventObject) {
        this.elasticPublisher.addEvent(eventObject);
    }

    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {

    }
}
