/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import java.util.Collections;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * The {@link ConstructGenericRecordsFn} class that converts KafkaMessages to Generic Records to
 * write it to Avro or Parquet File Format.
 */
public class ConstructGenericRecordsFn extends DoFn<KV<String, String>, GenericRecord> {

    /**
     * Generates the records using {@link GenericRecordBuilder}.
     */
    @ProcessElement
    public void processElement(ProcessContext c) {

        KV<String, String> message = c.element();
        String attributeKey = message.getKey();
        String attributeValue = message.getValue();

        Map<String, String> attributeMap;

        if (attributeValue != null) {
            if (attributeKey != null) {
                attributeMap = Collections.singletonMap(attributeKey, attributeValue);
            } else {
                attributeMap = Collections.singletonMap("", attributeValue);
            }
        } else {
            attributeMap = Collections.EMPTY_MAP;
        }

        c.output(
                new GenericRecordBuilder(WriteToGCSUtility.SCHEMA)
                        .set("message", attributeValue)
                        .set("attributes", attributeMap)
                        .set("timestamp", c.timestamp().getMillis())
                        .build());
    }
}
