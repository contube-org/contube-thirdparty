package com.zikeyang.contube.kafka.connect;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@Log4j2
public class KafkaWrappedSchema implements Schema<byte[]>, Serializable {

  private final SchemaInfo schemaInfo;

  public KafkaWrappedSchema(org.apache.avro.Schema schema, Converter converter) {
    Map<String, String> props = new HashMap<>();
    boolean isJsonConverter = converter instanceof JsonConverter;
    props.put(GenericAvroSchema.OFFSET_PROP, isJsonConverter ? "0" : "5");
    this.schemaInfo = SchemaInfo.builder()
        .name(isJsonConverter ? "KafKaJson" : "KafkaAvro")
        .type(isJsonConverter ? SchemaType.JSON : SchemaType.AVRO)
        .schema(schema.toString().getBytes(StandardCharsets.UTF_8))
        .properties(props)
        .build();
  }

  @Override
  public byte[] encode(byte[] data) {
    return data;
  }

  @Override
  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  @Override
  public Schema<byte[]> clone() {
    return null;
  }
}
