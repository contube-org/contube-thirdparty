package com.zikeyang.contube.pulsar.connect;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;

@Getter
public class PulsarConnectConfig {
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

  public SourceConfig convertToSourceConfig() {
    return SourceConfig.builder()
        .archive(archive)
        .className(className)
        .configs(connectorConfig)
        .build();
  }
}
