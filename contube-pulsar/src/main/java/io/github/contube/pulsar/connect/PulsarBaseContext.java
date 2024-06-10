package io.github.contube.pulsar.connect;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.functions.api.BaseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarBaseContext implements BaseContext {
  protected final String name;
  protected final PulsarConnectConfig config;
  protected final Logger instanceLog;

  public PulsarBaseContext(String name, PulsarConnectConfig config) {
    this.name = name;
    this.config = config;
    instanceLog =
        LoggerFactory.getILoggerFactory().getLogger(String.format("Pulsar-Sink-%s", name));
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
    return null;
  }

  @Override
  public String getSecret(String secretName) {
    return null;
  }

  @Override
  public void putState(String key, ByteBuffer value) {
    throw new UnsupportedOperationException("putState is not supported");
  }

  @Override
  public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
    throw new UnsupportedOperationException("putStateAsync is not supported");
  }

  @Override
  public ByteBuffer getState(String key) {
    throw new UnsupportedOperationException("getState is not supported");
  }

  @Override
  public CompletableFuture<ByteBuffer> getStateAsync(String key) {
    throw new UnsupportedOperationException("getStateAsync is not supported");
  }

  @Override
  public void deleteState(String key) {
    throw new UnsupportedOperationException("deleteState is not supported");
  }

  @Override
  public CompletableFuture<Void> deleteStateAsync(String key) {
    throw new UnsupportedOperationException("deleteStateAsync is not supported");
  }

  @Override
  public void incrCounter(String key, long amount) {
    throw new UnsupportedOperationException("incrCounter is not supported");
  }

  @Override
  public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
    throw new UnsupportedOperationException("incrCounterAsync is not supported");
  }

  @Override
  public long getCounter(String key) {
    throw new UnsupportedOperationException("getCounter is not supported");
  }

  @Override
  public CompletableFuture<Long> getCounterAsync(String key) {
    throw new UnsupportedOperationException("getCounterAsync is not supported");
  }

  @Override
  public void recordMetric(String metricName, double value) {
    instanceLog.info("Record metric: {} = {}", metricName, value);
  }
}
