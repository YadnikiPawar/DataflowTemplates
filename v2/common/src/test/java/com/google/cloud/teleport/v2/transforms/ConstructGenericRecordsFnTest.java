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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for {@link ConstructGenericRecordsFn}.
 */
@RunWith(JUnit4.class)
public class ConstructGenericRecordsFnTest {

    /**
     * Rule for pipeline testing.
     */
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    /**
     * Test whether {@link ConstructGenericRecordsFn} correctly maps the message.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testConstructGenericRecords() throws Exception {

        // Create the test input.
        final String key = "Name";
        final String value = "Generic";
        final KV<String, String> message = KV.of(key, value);

        // Register the coder for pipeline
        final FailsafeElementCoder<KV<String, String>, String> coder =
                FailsafeElementCoder.of(
                        KvCoder.of(
                                NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of())),
                        NullableCoder.of(StringUtf8Coder.of()));

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

        // Apply the ParDo.
        PCollection<GenericRecord> results =
                pipeline
                        .apply(
                                "CreateInput",
                                Create.of(message)
                                        .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
                        .apply("Generic record Creation", ParDo.of(new ConstructGenericRecordsFn()))
                        .setCoder(AvroCoder.of(GenericRecord.class, WriteToGCSUtility.SCHEMA));

        // Assert on the results.
        PAssert.that(results)
                .satisfies(
                        collection -> {
                            GenericRecord result = collection.iterator().next();
                            assertThat(result.get("message").toString(), is(equalTo(value)));
                            assertThat(result.get("attributes").toString(), is(equalTo("{Name=Generic}")));
                            return null;
                        });
        // Run the pipeline.
        pipeline.run();
    }
}
