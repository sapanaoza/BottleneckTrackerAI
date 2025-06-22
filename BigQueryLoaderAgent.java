package org.example.Agents;

import com.google.cloud.bigquery.*;
import jade.core.Agent;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JADE Agent responsible for loading processed machine bottleneck data
 * from Google Cloud Storage (JSONL or CSV format) into a BigQuery table.
 *
 * <p>This agent runs as part of a multi-agent system and performs
 * batch data ingestion tasks on startup, then terminates itself.</p>
 *
 * <p>Assumptions:
 * - Google Cloud credentials and environment are properly configured.
 * - BigQuery dataset and table exist and have appropriate permissions.
 * - Input files in GCS bucket are accessible.</p>
 */
public class BigQueryLoaderAgent extends Agent {

    // Logger for structured logging instead of System.out/err
    private static final Logger logger = Logger.getLogger(BigQueryLoaderAgent.class.getName());

    // BigQuery client initialized once per agent lifecycle
    private final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    // Constants: GCS URIs for input data (make configurable in prod)
    private static final String GCS_URI_JSONL = "gs://bottleneck-model-bucket/machine_data.jsonl";
    private static final String GCS_URI_CSV = "gs://bottleneck-model-bucket/machine_data.csv";

    // Constants: Target BigQuery dataset and table names (make configurable)
    private static final String DATASET_NAME = "bottleneck_data";
    private static final String TABLE_NAME = "machine_metrics";

    @Override
    protected void setup() {
        logger.info(getLocalName() + " started: Uploading bottleneck data to BigQuery for further analysis..");

        try {
            // Load JSONL data first; comment out CSV load if not needed
            loadDataFromGCSJSONL(DATASET_NAME, TABLE_NAME, GCS_URI_JSONL);

            // Load CSV data optionally
            //loadDataFromGCSCSV(DATASET_NAME, TABLE_NAME, GCS_URI_CSV);

        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, getLocalName() + ": Interrupted while waiting for BigQuery job.", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.log(Level.SEVERE, getLocalName() + ": Exception during BigQuery load job.", e);
        } finally {
            // Cleanly terminate this agent after job completion
            doDelete();
            logger.info(getLocalName() + " terminated.");
        }
    }

    /**
     * Loads newline-delimited JSON (JSONL) data from GCS into BigQuery.
     *
     * @param datasetName BigQuery dataset
     * @param tableName   BigQuery table
     * @param gcsUri      GCS URI of the JSONL file
     * @throws InterruptedException if the load job is interrupted
     */
    public void loadDataFromGCSJSONL(String datasetName, String tableName, String gcsUri) throws InterruptedException {
        TableId tableId = TableId.of(datasetName, tableName);

        // Explicit schema ensures consistent data structure
        Schema schema = getMachineMetricsSchema();

        // Configure BigQuery load job with JSON format and append mode
        LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, gcsUri)
                .setFormatOptions(FormatOptions.json())
                .setSchema(schema)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .setIgnoreUnknownValues(true)   // Skip fields not in schema
                .setMaxBadRecords(1)            // Allow 1 bad record before failure
                .setAutodetect(false)            // We provide explicit schema
                .build();

        runLoadJob(loadConfig, gcsUri);
    }

    /**
     * Loads CSV data from GCS into BigQuery, skipping header row.
     *
     * @param datasetName BigQuery dataset
     * @param tableName   BigQuery table
     * @param gcsUri      GCS URI of the CSV file
     * @throws InterruptedException if the load job is interrupted
     */
//    public void loadDataFromGCSCSV(String datasetName, String tableName, String gcsUri) throws InterruptedException {
//        TableId tableId = TableId.of(datasetName, tableName);
//
//        // Use explicit schema matching CSV structure
//        Schema schema = getMachineMetricsSchema();
//
//        CsvOptions csvOptions = CsvOptions.newBuilder()
//                .setSkipLeadingRows(1)  // Skip CSV header row
//                .build();
//
//        LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, gcsUri)
//                .setFormatOptions(csvOptions)
//                .setSchema(schema)
//                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
//                .setIgnoreUnknownValues(false)
//                .setMaxBadRecords(0)
//                .build();
//
//        runLoadJob(loadConfig, gcsUri);
//    }

    /**
     * Submits and waits for a BigQuery load job, logging detailed status.
     *
     * @param loadConfig Load job configuration
     * @param gcsUri     Source URI for logging
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    private void runLoadJob(LoadJobConfiguration loadConfig, String gcsUri) throws InterruptedException {
        logger.info(getLocalName() + ": Submitting load job for: " + gcsUri);

        // Create and start the job
        Job loadJob = bigQuery.create(JobInfo.of(loadConfig));

        // Wait synchronously for job completion
        loadJob = loadJob.waitFor();

        if (loadJob == null) {
            logger.severe(getLocalName() + ": Load job no longer exists.");
            return;
        }

        if (loadJob.isDone()) {
            if (loadJob.getStatus().getError() == null) {
                logger.info(getLocalName() + ": Data loaded successfully from: " + gcsUri);
            } else {
                logger.severe(getLocalName() + ": Load job failed with error: " +
                        loadJob.getStatus().getError().getMessage());

                if (loadJob.getStatus().getExecutionErrors() != null) {
                    loadJob.getStatus().getExecutionErrors().forEach(error ->
                            logger.severe("Execution error: " + error.getMessage()));
                }
            }
        } else {
            logger.severe(getLocalName() + ": Load job did not complete.");
        }
    }

    /**
     * Defines the schema for the machine bottleneck metrics table.
     * This schema must match the structure of the incoming data.
     *
     * @return BigQuery Schema object
     */
    private Schema getMachineMetricsSchema() {
        return Schema.of(
                Field.of("machineId", StandardSQLTypeName.STRING),
                Field.of("avgRuntime", StandardSQLTypeName.FLOAT64),
                Field.of("maxRuntime", StandardSQLTypeName.FLOAT64),
                Field.of("bottleneckRatio", StandardSQLTypeName.FLOAT64),
                Field.of("bottleneckScore", StandardSQLTypeName.FLOAT64),
                Field.of("alert", StandardSQLTypeName.BOOL),
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP) // Timestamp in ISO 8601 format or epoch millis
        );
    }
}
