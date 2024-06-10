package io.github.contube.pulsar;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;

@Getter
public class PulsarTubeConfig {
  @JsonProperty(required = true)
  private Map<String, Object> client;
}
