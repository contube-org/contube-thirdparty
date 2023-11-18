package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class Main {
    public static class TestMessage {
        public String name;
        public int id;
    }
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        SinkConfig sinkConfig =
                mapper.readValue(new File(args[0]), SinkConfig.class);
        String narArchive = sinkConfig.getArchive();
        String className = sinkConfig.getClassName();
        Map<String, Object> configs = sinkConfig.getConfigs();
        configs.put("batchMaxSize", 1);


//        NarClassLoader classLoader = NarClassLoaderBuilder.builder()
//                .narFile(new File(narArchive))
//                .extractionDirectory(Files.createTempDirectory("contube_pulsar_").toFile().getAbsolutePath())
//                .parentClassLoader(Thread.currentThread().getContextClassLoader())
//                .build();

//        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//        SinkConfig sinkConfig = mapper.readValue(new File(args[0]), SinkConfig.class);
//        LocalRunner localRunner = LocalRunner.builder()
//                .sinkConfig(sinkConfig).build();
//        try {
//            localRunner.start(true);
//        } catch (Exception e) {
//            localRunner.close();
//        }

        ClassLoader classLoader = extractClassLoader(narArchive);
        String sinkClassName = getSinkClassName(sinkConfig.getClassName(), classLoader);
        Class typeArg = getTypeClassName(classLoader.loadClass(sinkClassName));




        ClassLoader defaultClassLoader = Thread.currentThread().getContextClassLoader();

        Sink sink = (Sink) Reflections.createInstance(sinkClassName, classLoader);
        SinkContext sinkContext = createSinkContext(sinkConfig);
        Thread.currentThread().setContextClassLoader(classLoader);
        try{
            sink.open(configs, sinkContext);
        } catch (Exception e) {
            log.error("Sink open produced uncaught exception: ", e);
            sink.close();
        } finally {
            Thread.currentThread().setContextClassLoader(defaultClassLoader);
        }

        SchemaInfoProvider schemaInfoProvider = new SingleSchemaInfoProvider();
        AutoConsumeSchema schema = new AutoConsumeSchema();
//        schema.configureSchemaInfo("contube-pulsar", "value", Schema.STRING.getSchemaInfo());
        // Produce messages
        Schema internalSchema =  Schema.getSchema(Schema.STRING.getSchemaInfo());
        schema.setSchema(BytesSchemaVersion.of(new byte[0]), internalSchema); // BytesSchemaVersion.of(new byte[0]) is the default SV
//        schema.setSchema(BytesSchemaVersion.of("contube".getBytes()), Schema.getSchema(Schema.STRING.getSchemaInfo()));

        byte[] payload = Schema.STRING.encode("hello");
        GenericObject genericObject = schema.decode(payload, null);
        Record sinkRecord = new Record() {
            @Override
            public Object getValue() {
                return genericObject;
            }

            @Override
            public Schema getSchema() {
                return internalSchema;
            }
        };


        Thread.currentThread().setContextClassLoader(classLoader);
        try{
            sink.write(sinkRecord);
        } catch (Exception e) {
            log.info("Encountered exception in sink write: ", e);
            sink.close();
        } finally {
            Thread.currentThread().setContextClassLoader(defaultClassLoader);
        }

        Thread.sleep(8000);
    }

    static ClassLoader extractClassLoader(String userCodeFile) throws IOException, URISyntaxException {
        String narExtractionDirectory = Files.createTempDirectory("contube_pulsar_").toFile().getAbsolutePath();

        if (userCodeFile != null && Utils.isFunctionPackageUrlSupported(userCodeFile)) {
            File file = FunctionCommon.extractFileFromPkgURL(userCodeFile);
            return FunctionCommon.getClassLoaderFromPackage(
                    Function.FunctionDetails.ComponentType.SINK, null, file, narExtractionDirectory);
        } else if (userCodeFile != null) {
            File file = new File(userCodeFile);
            if (!file.exists()) {
                throw new RuntimeException("(" + userCodeFile + ") does not exist");
            }
            return FunctionCommon.getClassLoaderFromPackage(
                    Function.FunctionDetails.ComponentType.SINK, null, file, narExtractionDirectory);
        }
        return null;
    }

    static String getSinkClassName(String sinkClassName, ClassLoader sinkClassLoader) throws IOException {
        if(sinkClassName == null) {
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

        // TODO: We may need to extract typeArg from sinkClass
        return sinkClassName;
    }

    static Class<?> getTypeClassName(Class sinkClass) {
        return TypeResolver.resolveRawArgument(Sink.class, sinkClass);
    }

    static SinkContext createSinkContext(SinkConfig sinkConfig) {
        Logger instanceLog = LoggerFactory.getILoggerFactory().getLogger("test");

        return new SinkContext() {
            @Override
            public String getSinkName() {
                return "test-sink";
            }

            @Override
            public Collection<String> getInputTopics() {
                return Collections.singleton("contube-input");
            }

            @Override
            public SinkConfig getSinkConfig() {
                return sinkConfig;
            }

            @Override
            public String getTenant() {
                return "public";
            }

            @Override
            public String getNamespace() {
                return "default";
            }

            @Override
            public int getInstanceId() {
                return 0;
            }

            @Override
            public int getNumInstances() {
                return 1;
            }

            @Override
            public Logger getLogger() {
                return instanceLog;
            }

            @Override
            public String getSecret(String secretName) {
                return null;
            }

            @Override
            public void putState(String key, ByteBuffer value) {

            }

            @Override
            public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
                return null;
            }

            @Override
            public ByteBuffer getState(String key) {
                return null;
            }

            @Override
            public CompletableFuture<ByteBuffer> getStateAsync(String key) {
                return null;
            }

            @Override
            public void deleteState(String key) {

            }

            @Override
            public CompletableFuture<Void> deleteStateAsync(String key) {
                return null;
            }

            @Override
            public void incrCounter(String key, long amount) {

            }

            @Override
            public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
                return null;
            }

            @Override
            public long getCounter(String key) {
                return 0;
            }

            @Override
            public CompletableFuture<Long> getCounterAsync(String key) {
                return null;
            }

            @Override
            public void recordMetric(String metricName, double value) {

            }

            @Override
            public void fatal(Throwable t) {

            }
        };
    }

}
