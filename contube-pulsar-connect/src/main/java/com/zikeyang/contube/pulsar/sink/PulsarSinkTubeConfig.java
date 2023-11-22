package com.zikeyang.contube.pulsar.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zikeyang.contube.pulsar.PulsarTubeConfig;
import java.util.Map;
import lombok.Getter;

@Getter
public class PulsarSinkTubeConfig extends PulsarTubeConfig {
  @JsonProperty(required = true)
  private Map<String, Object> producer;
}
