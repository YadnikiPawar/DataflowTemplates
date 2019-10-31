package com.google.cloud.teleport.v2.transforms;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaToGCSOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToGCSOptions
        extends PipelineOptions,
        WriteToGCSText.WriteToGCSTextOptions,
        WriteToGCSParquet.WriteToGCSParquetOptions,
        WriteToGCSAvro.WriteToGCSAvroOptions {

    @Description("Kafka Bootstrap Server(s). ")
    @Validation.Required
    String getBootstrapServers();

    void setBootstrapServers(String bootstrapServers);

    @Description("Kafka topic(s) to read the input from. ")
    @Validation.Required
    String getInputTopics();

    void setInputTopics(String inputTopics);

    @Description(
            "The format of the file to write to. Default: TEXT. "
                    + "Allowed formats are: "
                    + "TEXT, AVRO, PARQUET")
    @Default.String("TEXT")
    String getOutputFileFormat();

    void setOutputFileFormat(String outputFileFormat);

    @Description(
            "The window duration in which data will be written. Defaults to 5m. "
                    + "Allowed formats are: "
                    + "Ns (for seconds, example: 5s), "
                    + "Nm (for minutes, example: 12m), "
                    + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);
}
