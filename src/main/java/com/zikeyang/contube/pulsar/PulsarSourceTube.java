package com.zikeyang.contube.pulsar;

import com.zikeyang.contube.api.Context;
import com.zikeyang.contube.api.Source;
import com.zikeyang.contube.api.TubeRecord;
import com.zikeyang.contube.common.Utils;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;

@Slf4j
public class PulsarSourceTube implements Source {
  PulsarSourceTubeConfig config;
  PulsarClient pulsarClient;
  Consumer<GenericRecord> consumer;
  AutoConsumeSchema schema;

  @SneakyThrows
  @Override
  public void open(Map<String, Object> map, Context context) {
    config = Utils.loadConfig(map, PulsarSourceTubeConfig.class);
    pulsarClient = PulsarClient.builder().loadConf(config.getClient()).build();
    schema = (AutoConsumeSchema) Schema.AUTO_CONSUME();
    consumer = pulsarClient.newConsumer(schema).loadConf(config.getConsumer()).subscribe();
  }

  @SneakyThrows
  @Override
  public TubeRecord read() {
    Message<GenericRecord> message = consumer.receive();
    log.info("Received message: {}", message.getMessageId());

    Schema<?> internalSchema = schema
        .unwrapInternalSchema(message.getSchemaVersion());
    byte[] schemaData =
        PulsarUtils.convertToSchemaProto(internalSchema.getSchemaInfo()).toByteArray();

    consumer.acknowledge(message);
    return PulsarTubeRecord.builder().value(message.getData()).schemaData(schemaData).build();
  }


  @Override
  public void close() throws Exception {
    pulsarClient.close();
  }
}
