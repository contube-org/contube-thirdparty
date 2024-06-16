package io.github.contube.pulsar.connect.source;

import io.github.contube.pulsar.connect.PulsarBaseContext;
import io.github.contube.pulsar.connect.PulsarConnectConfig;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.io.core.SourceContext;

public class PulsarSourceContext extends PulsarBaseContext implements SourceContext {
  final SourceConfig sourceConfig;

  public PulsarSourceContext(String name, PulsarConnectConfig config) {
    super(name, config);
    sourceConfig = config.convertToSourceConfig();
  }

  @Override
  public String getSourceName() {
    return name;
  }

  @Override
  public String getOutputTopic() {
    return "contube-topic";
  }

  @Override
  public SourceConfig getSourceConfig() {
    return sourceConfig;
  }

  @Override
  public <T> TypedMessageBuilder<T> newOutputMessage(String topicName, Schema<T> schema)
      throws PulsarClientException {
    throw new UnsupportedOperationException("newOutputMessage is not supported");
  }

  @Override
  public <T> ConsumerBuilder<T> newConsumerBuilder(Schema<T> schema) throws PulsarClientException {
    throw new UnsupportedOperationException("newConsumerBuilder is not supported");
  }
}
