package io.github.contube.pulsar.connect.sink;

import io.github.contube.api.TubeRecord;
import lombok.AllArgsConstructor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

@AllArgsConstructor
public class PulsarSinkRecord implements Record {
  private TubeRecord record;
  private Schema<?> schema;
  private Object value;

  @Override
  public Schema<?> getSchema() {
    return schema;
  }

  @Override
  public Object getValue() {
    return value;
  }



  @Override
  public void ack() {
    record.commit();
  }

  @Override
  public void fail() {
    throw new RuntimeException("Failed to process message.");
  }
}
