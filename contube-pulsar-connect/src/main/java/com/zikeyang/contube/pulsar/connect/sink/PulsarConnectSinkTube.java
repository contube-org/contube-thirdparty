package com.zikeyang.contube.pulsar.connect.sink;

import com.zikeyang.contube.api.Context;
import com.zikeyang.contube.api.Sink;
import com.zikeyang.contube.api.TubeRecord;
import com.zikeyang.contube.common.Utils;
import com.zikeyang.contube.pulsar.PulsarUtils;
import com.zikeyang.contube.pulsar.connect.PulsarConnectConfig;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.io.core.SinkContext;

@Slf4j
public class PulsarConnectSinkTube implements Sink {
  final AutoConsumeSchema schema = new AutoConsumeSchema();
  PulsarConnectConfig config;
  ClassLoader connectorClassLoader;
  org.apache.pulsar.io.core.Sink sink;
  Context context;

  @Override
  public void open(Map<String, Object> map, Context context) {
    this.context = context;
    config = Utils.loadConfig(map, PulsarConnectConfig.class);
    String narArchive = config.getArchive();
    Map<String, Object> connectorConfig = config.getConnectorConfig();
    try {
      connectorClassLoader =
          PulsarUtils.extractClassLoader(Function.FunctionDetails.ComponentType.SINK, narArchive);
      String sinkClassName =
          PulsarUtils.getSinkClassName(config.getClassName(), connectorClassLoader);
      sink = (org.apache.pulsar.io.core.Sink<?>) Reflections.createInstance(sinkClassName,
          connectorClassLoader);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load the connector", e);
    }
    SinkContext sinkContext = createSinkContext();
    ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(connectorClassLoader);
    try {
      sink.open(connectorConfig, sinkContext);
    } catch (Exception e) {
      log.error("Sink open produced uncaught exception: ", e);
      throw new RuntimeException("Unable to open the connector", e);
    } finally {
      Thread.currentThread().setContextClassLoader(defaultClassLoader);
    }
  }

  SinkContext createSinkContext() {
    return new PulsarSinkContext(context.getName(), config);
  }

  @SneakyThrows
  @Override
  public void write(TubeRecord record) {
    Record<?> sinkRecord = convertRecord(record);
    ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(connectorClassLoader);
    try {
      sink.write(sinkRecord);
    } catch (Exception e) {
      log.info("Encountered exception in sink write: ", e);
      throw e;
    } finally {
      Thread.currentThread().setContextClassLoader(defaultClassLoader);
    }
  }

  Record<?> convertRecord(TubeRecord record) {
    Schema<?> internalSchema = null;
    if (record.getSchemaData().isPresent()) {
      SchemaInfo schemaInfo = PulsarUtils.convertToSchemaInfo(record.getSchemaData().get());
      internalSchema = Schema.getSchema(schemaInfo);
      schema.setSchema(BytesSchemaVersion.of(new byte[0]), internalSchema);
    }
    GenericObject genericObject = schema.decode(record.getValue(), null);
    return new PulsarSinkRecord(record, internalSchema, genericObject);
  }

  @Override
  public void close() throws Exception {
    if (sink != null) {
      sink.close();
    }
  }
}
