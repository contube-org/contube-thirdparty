package com.zikeyang.contube.pulsar.connect.source;

import com.zikeyang.contube.api.Context;
import com.zikeyang.contube.api.Source;
import com.zikeyang.contube.api.TubeRecord;
import com.zikeyang.contube.common.Utils;
import com.zikeyang.contube.pulsar.PulsarTubeRecord;
import com.zikeyang.contube.pulsar.PulsarUtils;
import com.zikeyang.contube.pulsar.connect.PulsarConnectConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.io.core.SourceContext;

@Log4j2
public class PulsarConnectSourceTube implements Source {
  PulsarConnectConfig config;
  ClassLoader connectorClassLoader;
  org.apache.pulsar.io.core.Source source;
  Context context;

  @Override
  public void open(Map<String, Object> map, Context context) {
    this.context = context;
    config = Utils.loadConfig(map, PulsarConnectConfig.class);
    String narArchive = config.getArchive();
    Map<String, Object> connectorConfig = config.getConnectorConfig();
    try {
      connectorClassLoader =
          PulsarUtils.extractClassLoader(Function.FunctionDetails.ComponentType.SOURCE, narArchive);
      String sourceClassName =
          PulsarUtils.getSourceClassName(config.getClassName(), connectorClassLoader);
      source = (org.apache.pulsar.io.core.Source<?>) Reflections.createInstance(sourceClassName,
          connectorClassLoader);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load the connector", e);
    }
    SourceContext sourceContext = new PulsarSourceContext(context.getName(), config);
    ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(connectorClassLoader);
    try {
      source.open(connectorConfig, sourceContext);
    } catch (Exception e) {
      log.error("Source open produced uncaught exception: ", e);
      throw new RuntimeException("Unable to open the connector", e);
    } finally {
      Thread.currentThread().setContextClassLoader(defaultClassLoader);
    }
  }

  @SneakyThrows
  @Override
  public Collection<TubeRecord> read() {
    ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(connectorClassLoader);
    try {
      Record record = source.read();
      var tubeRecordBuilder = PulsarTubeRecord.builder();
      Schema schema = record.getSchema();
      if (schema != null) {
        byte[] schemaData =
            PulsarUtils.convertToSchemaProto(schema.getSchemaInfo()).toByteArray();
        tubeRecordBuilder.schemaData(schemaData);
        byte[] value = schema.encode(record.getValue());
        tubeRecordBuilder.value(value);
      } else {
        tubeRecordBuilder.value((byte[]) record.getValue());
      }
      return Collections.singletonList(tubeRecordBuilder.build());
    } catch (Exception e) {
      log.info("Encountered exception in source read: ", e);
      return null;
    } finally {
      Thread.currentThread().setContextClassLoader(defaultClassLoader);
    }
  }

  @Override
  public void close() throws Exception {
    if (source != null) {
      source.close();
    }
  }
}
