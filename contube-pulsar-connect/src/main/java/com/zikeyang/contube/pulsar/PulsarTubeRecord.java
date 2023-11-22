package com.zikeyang.contube.pulsar;

import com.zikeyang.contube.api.TubeRecord;
import java.util.Optional;
import lombok.Builder;

public class PulsarTubeRecord implements TubeRecord {
  private final byte[] value;
  private final byte[] schemaData;

  @Builder
  public PulsarTubeRecord(byte[] value, byte[] schemaData) {
    this.value = value;
    this.schemaData = schemaData;
  }

  @Override
  public byte[] getValue() {
    return this.value;
  }

  @Override
  public Optional<byte[]> getSchemaData() {
    return Optional.of(this.schemaData);
  }
}
