package io.github.contube.pulsar;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import org.apache.pulsar.client.impl.schema.SchemaInfoUtil;
import org.apache.pulsar.common.api.proto.Schema;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

public class PulsarUtils {
  public static Schema convertToSchemaProto(SchemaInfo schemaInfo) {
    Schema schema = new Schema();
    schema.setName(schemaInfo.getName())
        .setSchemaData(schemaInfo.getSchema())
        .setType(getSchemaType(schemaInfo.getType()));

    schemaInfo.getProperties().entrySet().stream().forEach(entry -> {
      if (entry.getKey() != null && entry.getValue() != null) {
        schema.addProperty()
            .setKey(entry.getKey())
            .setValue(entry.getValue());
      }
    });
    return schema;
  }

  public static SchemaInfo convertToSchemaInfo(byte[] schemaData) {
    Schema schema = new Schema();
    schema.parseFrom(schemaData);
    return SchemaInfoUtil.newSchemaInfo(schema);
  }

  private static org.apache.pulsar.common.api.proto.Schema.Type getSchemaType(SchemaType type) {
    if (type == SchemaType.AUTO_CONSUME) {
      return org.apache.pulsar.common.api.proto.Schema.Type.AutoConsume;
    } else if (type.getValue() < 0) {
      return org.apache.pulsar.common.api.proto.Schema.Type.None;
    } else {
      return org.apache.pulsar.common.api.proto.Schema.Type.valueOf(type.getValue());
    }
  }

  public static ClassLoader extractClassLoader(Function.FunctionDetails.ComponentType componentType,
                                               String userCodeFile)
      throws IOException, URISyntaxException {
    String narExtractionDirectory =
        Files.createTempDirectory("contube_pulsar_").toFile().getAbsolutePath();

    if (userCodeFile != null && Utils.isFunctionPackageUrlSupported(userCodeFile)) {
      File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
      return FunctionCommon.getClassLoaderFromPackage(
          componentType, null, file, narExtractionDirectory);
    } else if (userCodeFile != null) {
      File file = new File(userCodeFile);
      if (!file.exists()) {
        throw new RuntimeException("(" + userCodeFile + ") does not exist");
      }
      return FunctionCommon.getClassLoaderFromPackage(
          componentType, null, file, narExtractionDirectory);
    }
    return null;
  }

  public static String getSinkClassName(String sinkClassName, ClassLoader sinkClassLoader)
      throws IOException {
    if (sinkClassName == null) {
      sinkClassName = ConnectorUtils.getIOSinkClass((NarClassLoader) sinkClassLoader);
    }
    if (sinkClassName == null) {
      try {
        sinkClassName = ConnectorUtils.getIOSinkClass((NarClassLoader) sinkClassLoader);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to extract sink class from archive", e);
      }
    }
    // check if sink implements the correct interfaces
    Class sinkClass;
    try {
      sinkClass = sinkClassLoader.loadClass(sinkClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Sink class %s not found in class loader", sinkClassName), e);
    }

    return sinkClassName;
  }

  public static String getSourceClassName(String sourceClassName, ClassLoader sourceClassLoader)
      throws IOException {
    if (sourceClassName == null) {
      sourceClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) sourceClassLoader);
    }
    if (sourceClassName == null) {
      try {
        sourceClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) sourceClassLoader);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to extract source class from archive", e);
      }
    }
    // check if sink implements the correct interfaces
    try {
      sourceClassLoader.loadClass(sourceClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("Source class %s not found in class loader", sourceClassName), e);
    }

    return sourceClassName;
  }
}
