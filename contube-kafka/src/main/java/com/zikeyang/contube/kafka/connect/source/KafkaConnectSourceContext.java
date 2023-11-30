package com.zikeyang.contube.kafka.connect.source;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

@AllArgsConstructor
public class KafkaConnectSourceContext implements SourceTaskContext {
  private final Map<String, String> configs;
  private final OffsetStorageReader offsetStorageReader;
  @Override
  public Map<String, String> configs() {
    return configs;
  }

  @Override
  public OffsetStorageReader offsetStorageReader() {
    return offsetStorageReader;
  }
}
