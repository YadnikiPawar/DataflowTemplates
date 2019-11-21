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

import static com.google.cloud.teleport.bigtable.AvroToBigtable.toByteString;

import com.google.bigtable.v2.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Dataflow pipeline that imports data from Parquet files in GCS to a Cloud Bigtable table. The
 * Cloud Bigtable table must be created before running the pipeline and must have a compatible table
 * schema. For example, if {@link BigtableCell} from the Parquet files has a 'family' of "f1", the
 * Bigtable table should have a column family of "f1".
 */
public class ParquetToBigtable {

  /** Options for the import pipeline. */
  public interface Options extends PipelineOptions {
    @Description("The project that contains the table to import into.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to import into.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to import into.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description(
        "The input file patterm to read from. (e.g. gs://mybucket/somefolder/table1*.parquet)")
    ValueProvider<String> getInputFilePattern();

    @SuppressWarnings("unused")
    void setInputFilePattern(ValueProvider<String> inputFilePattern);
  }

  /**
   * Runs a pipeline to import Parquet files in GCS to a Cloud Bigtable table.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    BigtableIO.Write write =
        BigtableIO.write()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    pipeline
        .apply(
            "Read from Parquet",
            ParquetIO.read(BigtableRow.getClassSchema()).from(options.getInputFilePattern()))
        .apply("Transform to Bigtable", MapElements.via(new ParquetToBigtableFn()))
        .apply("Write to Bigtable", write);

    return pipeline.run();
  }

  /**
   * Translates {@link GenericRecord} to {@link Mutation}s along with a row key. The mutations are
   * {@link Mutation.SetCell}s that set the value for specified cells with family name, column
   * qualifier and timestamp.
   */
  static class ParquetToBigtableFn
      extends SimpleFunction<GenericRecord, KV<ByteString, Iterable<Mutation>>> {
    @Override
    public KV<ByteString, Iterable<Mutation>> apply(GenericRecord record) {
      ByteString key = toByteString((ByteBuffer) record.get(0));
      // BulkMutation doesn't split rows. Currently, if a single row contains more than 100,000
      // mutations, the service will fail the request.
      ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
      List<BigtableCell> cells = (List<BigtableCell>) record.get(1);
      for (BigtableCell cell : cells) {
        Mutation.SetCell setCell =
            Mutation.SetCell.newBuilder()
                .setFamilyName(cell.getFamily().toString())
                .setColumnQualifier(toByteString(cell.getQualifier()))
                .setTimestampMicros(cell.getTimestamp())
                .setValue(toByteString(cell.getValue()))
                .build();
        mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
      }
      return KV.of(key, mutations.build());
    }
  }
}
