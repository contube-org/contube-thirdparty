package com.zikeyang.contube.pulsar;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;

@Getter
public class PulsarSourceTubeConfig {
  @JsonProperty(required = true)
  private Map<String, Object> client;
  @JsonProperty(required = true)
  private Map<String, Object> consumer;
}
