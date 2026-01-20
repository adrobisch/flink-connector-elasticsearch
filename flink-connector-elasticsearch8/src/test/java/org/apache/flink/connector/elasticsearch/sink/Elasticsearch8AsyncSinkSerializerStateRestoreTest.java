/*
 *
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
 *
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.JsonData;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression test for snapshot restore when a request contains JsonData/JsonValue. */
class Elasticsearch8AsyncSinkSerializerStateRestoreTest {

    @Test
    void deserializeRestoresIndexOperationWithJsonDataDocument() throws Exception {
        IndexOperation<JsonData> indexOperation =
                new IndexOperation.Builder<JsonData>()
                        .id("index")
                        .index("testing")
                        .document(JsonData.fromJson("{\"action\":\"index\"}"))
                        .build();

        Operation operation = new Operation(indexOperation);
        long operationSize = new OperationSerializer().size(operation);
        BufferedRequestState<Operation> state =
                new BufferedRequestState<>(
                        Collections.singletonList(new RequestEntryWrapper<>(operation, operationSize)));

        Elasticsearch8AsyncSinkSerializer serializer = new Elasticsearch8AsyncSinkSerializer();
        byte[] serializedState = serializer.serialize(state);

        BufferedRequestState<Operation> restoredState =
                serializer.deserialize(serializer.getVersion(), serializedState);

        assertThat(restoredState.getBufferedRequestEntries()).hasSize(1);

        Operation restoredOperation = restoredState.getBufferedRequestEntries().get(0).getRequestEntry();
        assertThat(restoredOperation.getBulkOperationVariant()).isInstanceOf(IndexOperation.class);

        IndexOperation<?> restoredIndexOperation =
                (IndexOperation<?>) restoredOperation.getBulkOperationVariant();
        assertThat(restoredIndexOperation.id()).isEqualTo("index");
        assertThat(restoredIndexOperation.index()).isEqualTo("testing");
        assertThat(restoredIndexOperation.document()).isInstanceOf(JsonData.class);
        assertThat(((JsonData) restoredIndexOperation.document()).toJson().toString())
                .isEqualTo("{\"action\":\"index\"}");
    }
}
