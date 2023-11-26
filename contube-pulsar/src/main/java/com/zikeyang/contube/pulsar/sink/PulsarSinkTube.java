package com.zikeyang.contube.pulsar.sink;

import com.zikeyang.contube.api.Context;
import com.zikeyang.contube.api.Sink;
import com.zikeyang.contube.api.TubeRecord;
import com.zikeyang.contube.common.Utils;
import com.zikeyang.contube.pulsar.PulsarUtils;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

@Slf4j
public class PulsarSinkTube implements Sink {
  PulsarSinkTubeConfig config;
  PulsarClient pulsarClient;
  Producer<byte[]> producer;

  @SneakyThrows
  @Override
  public void open(Map<String, Object> map, Context context) {
    config = Utils.loadConfig(map, PulsarSinkTubeConfig.class);
    pulsarClient = PulsarClient.builder().loadConf(config.getClient()).build();
    producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
        .loadConf(config.getProducer()).create();
  }

  @Override
  public void write(TubeRecord tubeRecord) {
    Schema<?> schema = null;
    if (tubeRecord.getSchemaData().isPresent()) {
      SchemaInfo schemaInfo = PulsarUtils.convertToSchemaInfo(tubeRecord.getSchemaData().get());
      schema = Schema.getSchema(schemaInfo);
    }
    producer.newMessage(Schema.AUTO_PRODUCE_BYTES(schema))
        .value(tubeRecord.getValue())
        .sendAsync()
        .thenAccept(msgId -> {
              if (log.isDebugEnabled()) {
                log.debug("Message sent: {}", msgId);
              }
              tubeRecord.commit();
            }
        );
  }

  @Override
  public void close() throws Exception {
    if (pulsarClient != null) {
      pulsarClient.close();
    }
  }
}
