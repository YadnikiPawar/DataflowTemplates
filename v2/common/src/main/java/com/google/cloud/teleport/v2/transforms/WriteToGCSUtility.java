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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteToGCSUtility} class provides the static values and methods to handle various File
 * Formats.
 */
public class WriteToGCSUtility {

    /* Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(WriteToGCSUtility.class);

    private WriteToGCSUtility() {
    }

    /**
     * Set Enum FileFormat for all supported File formats.
     */
    public enum FileFormat {
        TEXT,
        AVRO,
        PARQUET;
    }

    /**
     * File Suffix to be set based on Format of the File.
     */
    static final ImmutableMap<FileFormat, String> FILE_SUFFIX_MAP =
            Maps.immutableEnumMap(
                    ImmutableMap.of(
                            FileFormat.TEXT, ".txt",
                            FileFormat.AVRO, ".avro",
                            FileFormat.PARQUET, ".parquet"));

    /**
     * Shard Template of the output file. Specified as repeating sequences of the letters 'S' or 'N'
     * (example: SSS-NNN).
     */
    static final String SHARD_TEMPLATE = "W-P-SS-of-NN";

    /**
     * Schema used for writing to Avro and Parquet Format.
     */
    private static final String SCHEMA_STRING =
            "{\n"
                    + " \"namespace\": \"com.google.cloud.teleport.v2.avro\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"GenericKafkaRecord\",\n"
                    + " \"fields\": [\n"
                    + "     {\"name\": \"message\",\"type\": \"string\"},\n"
                    + "     {\"name\": \"attributes\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
                    + "     {\"name\": \"timestamp\", \"type\": \"long\"}\n"
                    + " ]\n"
                    + "}";

    static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

    /**
     * The checkFileFormatFn() checks whether user specified File format is valid.
     *
     * @param fileFormat Format of the file given by the user
     */
    public static void checkFileFormatFn(String fileFormat) {
        try {
            FILE_SUFFIX_MAP.get(FileFormat.valueOf(fileFormat));
        } catch (Exception e) {
            LOG.info(
                    "Requested output file format is not a valid argument. File format needs to be either "
                            + Arrays.asList(FileFormat.values()).toString());
            throw new IllegalArgumentException(
                    "Requested output file format is not a valid argument. File format needs to be either "
                            + Arrays.asList(FileFormat.values()).toString());
        }
    }
}
