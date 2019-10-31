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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link FileFormatFactory} class is a {@link PTransform} that takes in {@link PCollection} of
 * kafka records.The Transform writes this records to GCS file(s) in user specified format.
 */
@AutoValue
public abstract class FileFormatFactory
        extends PTransform<PCollection<KV<String, String>>, POutput> {

    /* Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(FileFormatFactory.class);

    public static WriteToGCSBuilder newBuilder() {
        return new AutoValue_FileFormatFactory.Builder();
    }

    public abstract KafkaToGCSOptions options();

    public abstract String outputFileFormat();

    @Override
    public POutput expand(PCollection<KV<String, String>> records) {
        POutput output = null;
        WriteToGCSUtility.FileFormat outputFileFormat;
        try {
            outputFileFormat = WriteToGCSUtility.FileFormat.valueOf(outputFileFormat());
        } catch (Exception e) {
            LOG.info(
                    "Requested output file format is not a valid argument. File format needs to be either "
                            + Arrays.asList(WriteToGCSUtility.FileFormat.values()).toString());
            throw new IllegalArgumentException(
                    "Requested output file format is not a valid argument. File format needs to be either "
                            + Arrays.asList(WriteToGCSUtility.FileFormat.values()).toString());
        }
        /*
         * Calls appropriate Class Builder to performs PTransform based on user provided File Format.
         */
        switch (outputFileFormat) {
            case TEXT:
                output =
                        records.apply(
                                "Write Text File(s)",
                                WriteToGCSText.newBuilder()
                                        .withOutputDirectory(options().getOutputDirectory())
                                        .withOutputFilenamePrefix((options().getOutputFilenamePrefix()))
                                        .withNumShards(options().getNumShards())
                                        .withTempLocation(options().getTempLocation())
                                        .build());
                break;

            case AVRO:
                output =
                        records.apply(
                                "Write Avro File(s)",
                                WriteToGCSAvro.newBuilder()
                                        .withOutputDirectory(options().getOutputDirectory())
                                        .withOutputFilenamePrefix((options().getOutputFilenamePrefix()))
                                        .withNumShards(options().getNumShards())
                                        .withTempLocation(options().getTempLocation())
                                        .build());
                break;

            case PARQUET:
                output =
                        records.apply(
                                "Write Parquet File(s)",
                                WriteToGCSParquet.newBuilder()
                                        .withOutputDirectory(options().getOutputDirectory())
                                        .withOutputFilenamePrefix((options().getOutputFilenamePrefix()))
                                        .withNumShards(options().getNumShards())
                                        .build());
                break;

            default:
                break;
        }
        return output;
    }

    /**
     * Builder for {@link FileFormatFactory}.
     */
    @AutoValue.Builder
    public abstract static class WriteToGCSBuilder {

        public abstract WriteToGCSBuilder setOptions(KafkaToGCSOptions options);

        public abstract WriteToGCSBuilder setOutputFileFormat(String outputFileFormat);

        abstract KafkaToGCSOptions options();

        abstract FileFormatFactory autoBuild();

        public FileFormatFactory build() {
            setOutputFileFormat(options().getOutputFileFormat().toUpperCase());
            return autoBuild();
        }
    }
}
