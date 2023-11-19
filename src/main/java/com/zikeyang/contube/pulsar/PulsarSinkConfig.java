package com.zikeyang.contube.pulsar;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.io.SinkConfig;

@Getter
public class PulsarSinkConfig {
    @JsonProperty(required = true)
    private String archive;
    private String className;
    private Map<String, Object> connectorConfig;

    public SinkConfig convertToSinkConfig() {
        return SinkConfig.builder()
                .archive(archive)
                .className(className)
                .configs(connectorConfig)
                .build();
    }
}
