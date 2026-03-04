/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import co.elastic.clients.json.JsonData;

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/** Integration tests for {@link Elasticsearch8AsyncWriter}. */
public class Elasticsearch8AsyncWriterITCase extends ElasticsearchSinkBaseITCase {
    private TestSinkInitContext context;

    private final Lock lock = new ReentrantLock();
    private final Condition completed = lock.newCondition();
    private final AtomicInteger pendingCallbacks = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        this.context = new TestSinkInitContext();
    }

    @TestTemplate
    @Timeout(20)
    public void testBulkOnFlush() throws IOException, InterruptedException {
        String index = "test-bulk-on-flush";
        int maxBatchSize = 2;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);

            writer.flush(false);
            assertIdsAreWritten(index, new String[] {"test-1", "test-2"});

            writer.write(new DummyData("3", "test-3"), null);
            writer.flush(true);
            assertIdsAreWritten(index, new String[] {"test-3"});
        }
    }

    @TestTemplate
    @Timeout(20)
    public void testBulkOnBufferTimeFlush() throws Exception {
        String index = "test-bulk-on-time-in-buffer";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            writer.write(new DummyData("test-1", "test-1"), null);
            writer.flush(true);
            await();
            assertIdsAreWritten(index, new String[] {"test-1"});

            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            assertIdsAreNotWritten(index, new String[] {"test-2", "test-3"});
            context.getTestProcessingTimeService().advance(6000L);

            await();
        }

        assertIdsAreWritten(index, new String[] {"test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testBytesSentMetric() throws Exception {
        String index = "test-bytes-sent-metrics";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            assertThat(context.getNumBytesOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumBytesOutCounter().getCount()).isGreaterThan(0);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testRecordsSentMetric() throws Exception {
        String index = "test-records-sent-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(0);

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();
        }

        assertThat(context.getNumRecordsOutCounter().getCount()).isEqualTo(3);
        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testSendTimeMetric() throws Exception {
        String index = "test-send-time-metric";
        int maxBatchSize = 3;

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(index, maxBatchSize)) {
            final Optional<Gauge<Long>> currentSendTime = context.getCurrentSendTimeGauge();

            writer.write(new DummyData("test-1", "test-1"), null);
            writer.write(new DummyData("test-2", "test-2"), null);
            writer.write(new DummyData("test-3", "test-3"), null);

            await();

            assertThat(currentSendTime).isPresent();
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }

        assertIdsAreWritten(index, new String[] {"test-1", "test-2", "test-3"});
    }

    @TestTemplate
    @Timeout(10)
    public void testHandlePartiallyFailedBulk() throws Exception {
        String index = "test-partially-failed-bulk";
        int maxBatchSize = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) ->
                                new UpdateOperation.Builder<>()
                                        .id(element.getId())
                                        .index(index)
                                        .action(
                                                ac ->
                                                        ac.doc(element)
                                                                .docAsUpsert(
                                                                        element.getId()
                                                                                .equals("test-2")))
                                        .build());

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(maxBatchSize, elementConverter)) {
            writer.write(new DummyData("test-1", "test-1-updated"), null);
            writer.write(new DummyData("test-2", "test-2-updated"), null);
        }

        await();

        assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(1);
        assertIdsAreWritten(index, new String[] {"test-2"});
        assertIdsAreNotWritten(index, new String[] {"test-1"});
    }

    @TestTemplate
    @Timeout(20)
    public void testEmergencyModeDropsRecords() throws Exception {
        String index = "test-emergency-mode-drop";
        int maxBatchSize = 5;
        int maxRetries = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> poisonPillConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) -> {
                            if (element.getId().equals("poison-pill")) {
                                Map<String, Object> badDoc =
                                        Collections.singletonMap(
                                                "data",
                                                Collections.singletonMap("nested", "value"));
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(badDoc))
                                        .build();
                            } else {
                                Map<String, Object> goodDoc =
                                        Collections.singletonMap("data", "valid-payload-string");
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(goodDoc))
                                        .build();
                            }
                        });

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(maxBatchSize, poisonPillConverter, true, maxRetries)) {
            writer.write(new DummyData("valid-1", "valid-1"), null);
            writer.flush(true);
            assertIdsAreWritten(index, new String[] {"valid-1"});
            writer.write(new DummyData("valid-2", "valid-2"), null);
            writer.write(new DummyData("poison-pill", "poison"), null);
            writer.write(new DummyData("valid-3", "valid-3"), null);
            writer.flush(true);

            assertIdsAreWritten(index, new String[] {"valid-1", "valid-2", "valid-3"});
            assertIdsAreNotWritten(index, new String[] {"poison-pill"}); // PROOF it was dropped
            waitForMetric(
                    () -> context.metricGroup().getNumRecordsOutErrorsCounter().getCount() > 0);
            assertThat(context.metricGroup().getNumRecordsOutErrorsCounter().getCount())
                    .isGreaterThan(0);
        }
    }

    @TestTemplate
    @Timeout(20)
    public void testNonEmergencyModeFailsJob() throws Exception {
        String index = "test-non-emergency-fail";
        int maxBatchSize = 5;
        int maxRetries = 2;

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> poisonPillConverter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        (element, ctx) -> {
                            if (element.getId().equals("poison-pill")) {
                                Map<String, Object> badDoc =
                                        Collections.singletonMap(
                                                "data",
                                                Collections.singletonMap("nested", "value"));
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(badDoc))
                                        .build();
                            } else {
                                Map<String, Object> goodDoc =
                                        Collections.singletonMap("data", "valid-payload-string");
                                return new IndexOperation.Builder<>()
                                        .index(index)
                                        .id(element.getId())
                                        .document(JsonData.of(goodDoc))
                                        .build();
                            }
                        });

        try (final Elasticsearch8AsyncWriter<DummyData> writer =
                createWriter(maxBatchSize, poisonPillConverter, false, maxRetries)) {
            writer.write(new DummyData("valid-1", "valid-1"), null);
            writer.flush(true);
            writer.write(new DummyData("poison-pill", "poison"), null);
            assertThrows(FlinkRuntimeException.class, () -> writer.flush(true));
        }
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(String index, int maxBatchSize)
            throws IOException {
        return createWriter(
                maxBatchSize,
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        getElementConverterForDummyData(index)),
                false,
                5);
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(
            int maxBatchSize,
            Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter)
            throws IOException {
        return createWriter(maxBatchSize, elementConverter, false, 5);
    }

    private NetworkConfig createNetworkConfig() {
        final List<HttpHost> esHost = Collections.singletonList(getHost());
        return secure
                ? new NetworkConfig(
                        esHost,
                        ES_CLUSTER_USERNAME,
                        ES_CLUSTER_PASSWORD,
                        null,
                        () -> ES_CONTAINER_SECURE.createSslContextFromCa(),
                        null)
                : new NetworkConfig(esHost, null, null, null, null, null);
    }

    private Elasticsearch8AsyncWriter<DummyData> createWriter(
            int maxBatchSize,
            Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> elementConverter,
            boolean emergencyMode,
            int maxRetries)
            throws IOException {

        Elasticsearch8AsyncSink<DummyData> sink =
                new Elasticsearch8AsyncSink<DummyData>(
                        elementConverter,
                        maxBatchSize,
                        50,
                        10_000,
                        5 * 1024 * 1024,
                        5000,
                        1024 * 1024,
                        createNetworkConfig(),
                        emergencyMode,
                        maxRetries) {
                    @Override
                    public StatefulSinkWriter<DummyData, BufferedRequestState<RetryableOperation>>
                            createWriter(WriterInitContext context) {
                        return new Elasticsearch8AsyncWriter<DummyData>(
                                getElementConverter(),
                                context,
                                maxBatchSize,
                                getMaxInFlightRequests(),
                                getMaxBufferedRequests(),
                                getMaxBatchSizeInBytes(),
                                getMaxTimeInBufferMS(),
                                getMaxRecordSizeInBytes(),
                                networkConfig,
                                Collections.emptyList(),
                                emergencyMode,
                                maxRetries) {
                            @Override
                            protected void submitRequestEntries(
                                    List<RetryableOperation> requestEntries,
                                    ResultHandler<RetryableOperation> resultHandler) {

                                ResultHandler<RetryableOperation> wrappedHandler =
                                        new ResultHandler<RetryableOperation>() {
                                            @Override
                                            public void complete() {
                                                resultHandler.complete();
                                                signal();
                                            }

                                            @Override
                                            public void completeExceptionally(Exception e) {
                                                resultHandler.completeExceptionally(e);
                                                signal();
                                            }

                                            @Override
                                            public void retryForEntries(
                                                    List<RetryableOperation> list) {
                                                resultHandler.retryForEntries(list);
                                                signal();
                                            }
                                        };
                                try {
                                    super.submitRequestEntries(requestEntries, wrappedHandler);
                                } catch (Exception e) {
                                    wrappedHandler.completeExceptionally(e);
                                }
                            }
                        };
                    }
                };

        return (Elasticsearch8AsyncWriter<DummyData>) sink.createWriter(context);
    }

    private void signal() {
        lock.lock();
        try {
            pendingCallbacks.incrementAndGet();
            completed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void await() throws InterruptedException {
        lock.lock();
        try {
            while (pendingCallbacks.get() == 0) {
                if (!completed.await(10, TimeUnit.SECONDS)) {
                    throw new java.util.concurrent.TimeoutException(
                            "Timed out waiting for AsyncSinkWriter callback");
                }
            }
            pendingCallbacks.decrementAndGet();
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Verifies that a <b>502 Bad Gateway</b> response from Elasticsearch (or an upstream proxy) is
     * treated as a transient error: the writer must call {@code retryForEntries()} so that all
     * affected records are re-queued for retry rather than silently dropped or causing an immediate
     * job failure.
     *
     * <p>Mechanism: a tiny {@link ServerSocket} in the test always replies with a raw {@code
     * HTTP/1.1 502 Bad Gateway} — no external library needed.
     */
    @TestTemplate
    @Timeout(20)
    public void testBadGateway502CausesRetry() throws Exception {
        AtomicBoolean retriedCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionCalled = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int fakePort = serverSocket.getLocalPort();

            // Fake HTTP server: always responds 502.
            Thread fakeServer =
                    new Thread(
                            () -> {
                                for (int i = 0; i < 10; i++) {
                                    try {
                                        Socket conn = serverSocket.accept();
                                        byte[] buf = new byte[4096];
                                        conn.getInputStream().read(buf);
                                        String response =
                                                "HTTP/1.1 502 Bad Gateway\r\n"
                                                        + "Content-Length: 11\r\n"
                                                        + "Content-Type: text/plain\r\n"
                                                        + "Connection: close\r\n"
                                                        + "\r\n"
                                                        + "Bad Gateway";
                                        OutputStream out = conn.getOutputStream();
                                        out.write(response.getBytes(StandardCharsets.UTF_8));
                                        out.flush();
                                        conn.close();
                                    } catch (Exception ignored) {
                                        break;
                                    }
                                }
                            },
                            "fake-502-server");
            fakeServer.setDaemon(true);
            fakeServer.start();

            HttpHost fakeHost = new HttpHost("localhost", fakePort, "http");

            // Call submitRequestEntries directly – avoids the flush/threading complexity
            // of AsyncSinkWriter while still exercising the real HTTP + error-handling path.
            invokeSubmitAndCapture(fakeHost, retriedCalled, exceptionCalled, latch);

            boolean callbackReceived = latch.await(15, TimeUnit.SECONDS);
            assertThat(callbackReceived).as("ResultHandler must be called within timeout").isTrue();
        }

        assertThat(retriedCalled.get())
                .as(
                        "A 502 Bad Gateway must trigger retryForEntries() – records should be"
                            + " retried, not lost")
                .isTrue();
        assertThat(exceptionCalled.get())
                .as(
                        "A 502 Bad Gateway must NOT immediately fail the job via"
                            + " completeExceptionally()")
                .isFalse();
    }

    /**
     * Verifies that a <b>connection reset</b> (simulating a connection timeout or a crashed ES node
     * closing the TCP connection mid-flight) is treated as a transient error: the writer must call
     * {@code retryForEntries()} so that all affected records are re-queued for retry.
     *
     * <p>Mechanism: a tiny {@link ServerSocket} accepts the connection and immediately closes it
     * without writing a single byte, causing the HTTP client to receive an {@link
     * java.io.IOException} — identical to what happens on a real connection timeout or reset.
     */
    @TestTemplate
    @Timeout(20)
    public void testConnectionResetCausesRetry() throws Exception {
        AtomicBoolean retriedCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionCalled = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int fakePort = serverSocket.getLocalPort();

            // Fake server: accepts and immediately closes – simulates connection reset / timeout.
            Thread fakeServer =
                    new Thread(
                            () -> {
                                for (int i = 0; i < 10; i++) {
                                    try {
                                        Socket conn = serverSocket.accept();
                                        conn.close();
                                    } catch (Exception ignored) {
                                        break;
                                    }
                                }
                            },
                            "fake-reset-server");
            fakeServer.setDaemon(true);
            fakeServer.start();

            HttpHost fakeHost = new HttpHost("localhost", fakePort, "http");

            invokeSubmitAndCapture(fakeHost, retriedCalled, exceptionCalled, latch);

            boolean callbackReceived = latch.await(15, TimeUnit.SECONDS);
            assertThat(callbackReceived).as("ResultHandler must be called within timeout").isTrue();
        }

        assertThat(retriedCalled.get())
                .as(
                        "A connection reset must trigger retryForEntries() – records should be"
                            + " retried, not lost")
                .isTrue();
        assertThat(exceptionCalled.get())
                .as(
                        "A connection reset must NOT immediately fail the job via"
                            + " completeExceptionally()")
                .isFalse();
    }

    /**
     * Builds a real {@link Elasticsearch8AsyncWriter} pointing at {@code host}, then calls {@link
     * Elasticsearch8AsyncWriter#submitRequestEntries} directly with two dummy records. The provided
     * {@link ResultHandler} captures which outcome path was taken:
     *
     * <ul>
     *   <li>{@code retriedCalled} → set when {@code retryForEntries()} is invoked
     *   <li>{@code exceptionCalled} → set when {@code completeExceptionally()} is invoked
     * </ul>
     *
     * {@code latch} is counted down on the first callback so the calling test can block on it.
     */
    private void invokeSubmitAndCapture(
            HttpHost host,
            AtomicBoolean retriedCalled,
            AtomicBoolean exceptionCalled,
            CountDownLatch latch)
            throws IOException {

        NetworkConfig networkConfig =
                new NetworkConfig(Collections.singletonList(host), null, null, null, null, null);

        Elasticsearch8AsyncSinkBuilder.OperationConverter<DummyData> converter =
                new Elasticsearch8AsyncSinkBuilder.OperationConverter<>(
                        getElementConverterForDummyData("test-error-index"));

        // Build two operations to submit.
        RetryableOperation op1 = converter.apply(new DummyData("err-1", "name-1"), null);
        RetryableOperation op2 = converter.apply(new DummyData("err-2", "name-2"), null);
        List<RetryableOperation> ops = new java.util.ArrayList<>();
        ops.add(op1);
        ops.add(op2);

        // Create the writer directly (not via the sink) so we can call submitRequestEntries.
        Elasticsearch8AsyncWriter<DummyData> writer =
                new Elasticsearch8AsyncWriter<>(
                        converter,
                        context,
                        10,
                        1,
                        100,
                        5 * 1024 * 1024,
                        5_000,
                        1024 * 1024,
                        networkConfig,
                        Collections.emptyList(),
                        false,
                        3);

        ResultHandler<RetryableOperation> capturingHandler =
                new ResultHandler<RetryableOperation>() {
                    @Override
                    public void complete() {
                        latch.countDown();
                    }

                    @Override
                    public void completeExceptionally(Exception e) {
                        exceptionCalled.set(true);
                        latch.countDown();
                    }

                    @Override
                    public void retryForEntries(List<RetryableOperation> list) {
                        retriedCalled.set(true);
                        latch.countDown();
                    }
                };

        // submitRequestEntries is protected – call it directly from within this package-private
        // test class (same package as the writer, so access is permitted).
        writer.submitRequestEntries(ops, capturingHandler);
    }

    private void waitForMetric(Supplier<Boolean> condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        lock.lock();
        try {
            while (!condition.get()) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("Timeout waiting for metric condition");
                }
                completed.await(100, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }
}
