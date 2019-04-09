package io.simplesource.saga.shared.kafka;

import io.simplesource.saga.shared.streams.StreamAppConfig;

import java.util.Map;
import java.util.Properties;

public class PropertiesBuilder {
    private Properties defaults;
    private final Properties properties = new Properties();

    PropertiesBuilder(Properties defaults) {
        this.defaults = defaults;
    }

    public static PropertiesBuilder create() {
        return new PropertiesBuilder(new Properties());
    }

    public static PropertiesBuilder withDefaults(Properties defaults) {
        return new PropertiesBuilder(defaults);
    }

    public PropertiesBuilder withProperty(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    public PropertiesBuilder withProperties(Properties properties) {
        properties.forEach(this.properties::put);
        return this;
    }

    public PropertiesBuilder withProperties(Map<String, String> properties) {
        properties.forEach(this.properties::put);
        return this;
    }

    public PropertiesBuilder withStreamAppConfig(StreamAppConfig appConfig) {
        Properties properties = StreamAppConfig.getConfig(appConfig);
        withProperties(properties);
        return this;
    }

    public Properties build() {
        Properties newProps = KafkaUtils.copyProperties(defaults);
        properties.forEach(newProps::put) ;
        return newProps;
    }

    public interface BuildSteps {
        PropertiesBuilder applyStep(PropertiesBuilder builder);

        public default BuildSteps withNextStep(BuildSteps initial) {
            return builder -> initial.applyStep(this.applyStep(builder));
        }

        public default BuildSteps withInitialStep(BuildSteps initial) {
            return builder -> this.applyStep(initial.applyStep(builder));
        }

        public default Properties build() {
            return this.applyStep(PropertiesBuilder.create()).build();
        }
    }
}
