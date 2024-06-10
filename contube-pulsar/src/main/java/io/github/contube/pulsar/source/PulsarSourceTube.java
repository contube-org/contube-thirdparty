package io.github.contube.pulsar.source;

import io.github.contube.api.Context;
import io.github.contube.api.Source;
import io.github.contube.api.TubeRecord;
import io.github.contube.common.Utils;
import io.github.contube.pulsar.PulsarTubeRecord;
import io.github.contube.pulsar.PulsarUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;

@Slf4j
public class PulsarSourceTube implements Source {
  PulsarSourceTubeConfig config;
  PulsarClient pulsarClient;
  Consumer<GenericRecord> consumer;

  @SneakyThrows
  @Override
  public void open(Map<String, Object> map, Context context) {
    config = Utils.loadConfig(map, PulsarSourceTubeConfig.class);
    pulsarClient = PulsarClient.builder().loadConf(config.getClient()).build();
    consumer =
        pulsarClient.newConsumer(Schema.AUTO_CONSUME()).loadConf(config.getConsumer()).subscribe();
  }

  @SneakyThrows
  @Override
  public Collection<TubeRecord> read() {
    Message<GenericRecord> message = consumer.receive();
    log.info("Received message: {}", message.getMessageId());

    var recordBuilder =
        PulsarTubeRecord.builder().value(message.getData());

    if (message.getReaderSchema().isPresent()) {
      Schema<?> readerSchema = message.getReaderSchema().get();
      byte[] schemaData =
          PulsarUtils.convertToSchemaProto(readerSchema.getSchemaInfo()).toByteArray();
      recordBuilder.schemaData(schemaData);
    }

    TubeRecord record = recordBuilder.build();
    record.waitForCommit()
        .thenRun(() -> {
          if (log.isDebugEnabled()) {
            log.info("Message has been commited: {}", message.getMessageId());
          }
          try {
            consumer.acknowledge(message);
          } catch (PulsarClientException e) {
            log.error("Failed to acknowledge message: {}", message.getMessageId(), e);
          }
        });
    return Collections.singletonList(record);
  }

  @Override
  public void close() throws Exception {
    if (pulsarClient != null) {
      pulsarClient.close();
    }
  }
}
