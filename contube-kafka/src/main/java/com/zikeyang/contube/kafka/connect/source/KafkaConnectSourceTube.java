package com.zikeyang.contube.kafka.connect.source;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.zikeyang.contube.api.Context;
import com.zikeyang.contube.api.Source;
import com.zikeyang.contube.api.TubeRecord;
import com.zikeyang.contube.common.RawRecord;
import com.zikeyang.contube.kafka.connect.KafkaWrappedSchema;
import com.zikeyang.contube.pulsar.PulsarUtils;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

@Log4j2
public class KafkaConnectSourceTube implements Source {

  private static final String JSON_WITH_ENVELOPE_CONFIG = "json-with-envelope";
  private static final String CONNECTOR_CLASS = "kafkaConnectorSourceClass";
  private static final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
  private static final AvroData avroData = new AvroData(1000);
  private boolean jsonWithEnvelope = false;
  public Converter keyConverter;
  public Converter valueConverter;
  private OffsetBackingStore offsetStore;
  private OffsetStorageReader offsetReader;
  private OffsetStorageWriter offsetWriter;
  private SourceTaskContext sourceTaskContext;
  private SourceConnector connector;
  private SourceTask sourceTask;

  @Override
  public void open(Map<String, Object> config, Context context) {
    if (config.get(JSON_WITH_ENVELOPE_CONFIG) != null) {
      jsonWithEnvelope = Boolean.parseBoolean(config.get(JSON_WITH_ENVELOPE_CONFIG).toString());
      log.info("Using jsonWithEnvelope: {}", jsonWithEnvelope);
      config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, jsonWithEnvelope);
    } else {
      config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    }

    Map<String, String> stringConfig = new HashMap<>();
    config.forEach((key, value) -> {
      if (value instanceof String) {
        stringConfig.put(key, (String) value);
      }
    });

    stringConfig.putIfAbsent(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
    stringConfig.putIfAbsent(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);

    try {
      keyConverter = Class.forName(stringConfig.get(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG))
          .asSubclass(Converter.class)
          .getDeclaredConstructor()
          .newInstance();
      valueConverter = Class.forName(stringConfig.get(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG))
          .asSubclass(Converter.class)
          .getDeclaredConstructor()
          .newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize converters", e);
    }

    if (keyConverter instanceof AvroConverter) {
      keyConverter = new AvroConverter(new MockSchemaRegistryClient());
      config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
    }
    if (valueConverter instanceof AvroConverter) {
      valueConverter = new AvroConverter(new MockSchemaRegistryClient());
      config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
    }
    keyConverter.configure(config, true);
    valueConverter.configure(config, false);

    offsetStore = new FileOffsetBackingStore(keyConverter);
    stringConfig.putIfAbsent(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
        context.getName() + "-offset");
    offsetStore.configure(new StandaloneConfig(stringConfig));
    offsetStore.start();

    offsetReader = new OffsetStorageReaderImpl(
        offsetStore,
        "pulsar-kafka-connect-adaptor",
        keyConverter,
        valueConverter
    );
    offsetWriter = new OffsetStorageWriter(
        offsetStore,
        "pulsar-kafka-connect-adaptor",
        keyConverter,
        valueConverter
    );

    sourceTaskContext = new KafkaConnectSourceContext(stringConfig, offsetReader);

    final Map<String, String> taskConfig;
    try {
      if (config.get(CONNECTOR_CLASS) != null) {
        String kafkaConnectorFQClassName = config.get(CONNECTOR_CLASS).toString();
        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SourceConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sourceTask = (SourceTask) taskClass.getConstructor().newInstance();

        connector.initialize(new ConnectorContext() {
          @Override
          public void requestTaskReconfiguration() {
            throw new UnsupportedOperationException("requestTaskReconfiguration is not supported");
          }

          @Override
          public void raiseError(Exception e) {
            throw new UnsupportedOperationException("raiseError is not supported");
          }
        });
        connector.start(stringConfig);

        List<Map<String, String>> configs = connector.taskConfigs(1);
        checkNotNull(configs);
        checkArgument(configs.size() == 1);
        taskConfig = configs.get(0);
      } else {
        sourceTask = Class.forName(stringConfig.get(TaskConfig.TASK_CLASS_CONFIG))
            .asSubclass(SourceTask.class)
            .getDeclaredConstructor()
            .newInstance();
        taskConfig = stringConfig;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize connector and task", e);
    }

    sourceTask.initialize(sourceTaskContext);
    sourceTask.start(taskConfig);

  }

  @SneakyThrows
  @Override
  public TubeRecord read() {
    List<SourceRecord> recordList;
    while (true) {
      recordList = sourceTask.poll();
      if (recordList != null && !recordList.isEmpty()) {
        break;
      }
      Thread.sleep(1000);
    }

    for (SourceRecord srcRecord : recordList) {
      offsetWriter.offset(srcRecord.sourcePartition(), srcRecord.sourceOffset());
      RawRecord.RawRecordBuilder rawRecordBuilder = RawRecord.builder();
      byte[] valueBytes = valueConverter.fromConnectData(
          srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());
      rawRecordBuilder.value(valueBytes);
      KafkaWrappedSchema valueSchema = new KafkaWrappedSchema(
          avroData.fromConnectSchema(srcRecord.valueSchema()), valueConverter);
      rawRecordBuilder.schemaData(
          Optional.of(PulsarUtils.convertToSchemaProto(valueSchema.getSchemaInfo()).toByteArray()));
      // TODO: Only for demo purpose: return the first record
      RawRecord record = rawRecordBuilder.build();
      record.waitForCommit().thenRun(() -> {
        try {
          sourceTask.commitRecord(srcRecord, null);
        } catch (InterruptedException e) {
          log.info("Commit record failed", e);
        }
      });
      return record;
    }
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
