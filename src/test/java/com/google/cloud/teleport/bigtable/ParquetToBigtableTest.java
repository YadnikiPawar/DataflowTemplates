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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.ParquetToBigtable.ParquetToBigtableFn;
import static com.google.cloud.teleport.bigtable.TestUtils.addBigtableMutation;
import static com.google.cloud.teleport.bigtable.TestUtils.addParquetCell;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRowMutations;

import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ParquetToBigtable. */
@RunWith(JUnit4.class)
public class ParquetToBigtableTest {

//  private static final String PARQUET_RESOURCE_DIR = "ParquetToBigtableTest/";
//  private static final ValueProvider<String> SOURCE_FILE_PATH =
//      ValueProvider.StaticValueProvider.of(
//          Resources.getResource(PARQUET_RESOURCE_DIR + "parquetToBigtableTest.parquet").getPath());

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void applyParquettoBigtableFn() throws Exception {

    byte[] rowKey1 = "row1".getBytes();
    ByteBuffer key1 = ByteBuffer.wrap(rowKey1);
    List<BigtableCell> cells1 = new ArrayList<>();
    addParquetCell(cells1, "family1", "column1", 1, "value1");
    addParquetCell(cells1, "family1", "column1", 2, "value2");
    addParquetCell(cells1, "family1", "column2", 1, "value3");
    addParquetCell(cells1, "family2", "column1", 1, "value4");
    GenericRecord parquetRow1 =
        new GenericRecordBuilder(BigtableRow.getClassSchema())
            .set("key", key1)
            .set("cells", cells1)
            .build();

    byte[] rowKey2 = "row2".getBytes();
    ByteBuffer key2 = ByteBuffer.wrap(rowKey2);
    List<BigtableCell> cells2 = new ArrayList<>();
    addParquetCell(cells2, "family2", "column2", 2, "value2");
    GenericRecord parquetRow2 =
        new GenericRecordBuilder(BigtableRow.getClassSchema())
            .set("key", key2)
            .set("cells", cells2)
            .build();
    final List<GenericRecord> parquetRows = ImmutableList.of(parquetRow1, parquetRow2);

//    ParquetToBigtable.Options options =
//        PipelineOptionsFactory.create().as(ParquetToBigtable.Options.class);
//    options.setInputFilePattern(SOURCE_FILE_PATH);

    KV<ByteString, Iterable<Mutation>> rowMutations1 = createBigtableRowMutations("row1");
    addBigtableMutation(rowMutations1, "family1", "column1", 1, "value1");
    addBigtableMutation(rowMutations1, "family1", "column1", 2, "value2");
    addBigtableMutation(rowMutations1, "family1", "column2", 1, "value3");
    addBigtableMutation(rowMutations1, "family2", "column1", 1, "value4");
    KV<ByteString, Iterable<Mutation>> rowMutations2 = createBigtableRowMutations("row2");
    addBigtableMutation(rowMutations2, "family2", "column2", 2, "value2");
    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(rowMutations1, rowMutations2);

    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
//            .apply(
//                "Read from Parquet",
//                ParquetIO.read(BigtableRow.getClassSchema()).from(options.getInputFilePattern()))
            .apply(
                "Create",
                Create.of(parquetRows)
                    .withCoder(AvroCoder.of(GenericRecord.class, BigtableRow.getClassSchema())))
            .apply("Transform to Bigtable", MapElements.via(new ParquetToBigtableFn()));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }
}
