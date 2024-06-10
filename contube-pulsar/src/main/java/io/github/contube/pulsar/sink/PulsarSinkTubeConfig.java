package io.github.contube.pulsar.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.contube.pulsar.PulsarTubeConfig;
import java.util.Map;
import lombok.Getter;

@Getter
public class PulsarSinkTubeConfig extends PulsarTubeConfig {
  @JsonProperty(required = true)
  private Map<String, Object> producer;
}
