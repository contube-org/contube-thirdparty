package com.zikeyang.contube.pulsar.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zikeyang.contube.pulsar.PulsarTubeConfig;
import java.util.Map;
import lombok.Getter;

@Getter
public class PulsarSourceTubeConfig extends PulsarTubeConfig {
  @JsonProperty(required = true)
  private Map<String, Object> consumer;
}
