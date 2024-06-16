package io.github.contube.pulsar.connect.sink;

import io.github.contube.pulsar.connect.PulsarBaseContext;
import io.github.contube.pulsar.connect.PulsarConnectConfig;
import java.util.Collection;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.io.core.SinkContext;

public class PulsarSinkContext extends PulsarBaseContext implements SinkContext {
  final SinkConfig sinkConfig;

  public PulsarSinkContext(String name, PulsarConnectConfig config) {
    super(name, config);
    this.sinkConfig = config.convertToSinkConfig();

  }
  @Override
  public String getSinkName() {
    return name;
  }

  @Override
  public Collection<String> getInputTopics() {
    return Collections.singleton("contube-topic");
  }

  @Override
  public SinkConfig getSinkConfig() {
    return sinkConfig;
  }
}
