package org.example.Agents;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import jade.core.Agent;

/**
 * JADE agent responsible for loading data from a Google Cloud Storage (GCS) JSONL file
 * into a BigQuery table.
 *
 * This agent uses the Google Cloud BigQuery client library to perform
 * a load job, appending data to the specified BigQuery table.
 *
 * The agent terminates itself after the loading operation is complete.
 */
public class BigQueryLoaderAgent extends Agent {

    // BigQuery service client, initialized with default credentials/environment
    private final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    // (Optional) local CSV file path for reference or future use
    private static final String CSV_FILE_PATH = "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.csv";

    /**
     * JADE lifecycle setup method.
     * This method is called automatically when the agent starts.
     */
    @Override
    protected void setup() {
        System.out.println(getLocalName() + "Uploads processed bottleneck data to Google BigQuery for analytics....");

        // Define dataset, table, and GCS URI parameters
        final String datasetName = "bottleneck_data";
        final String tableName = "machine_metrics";
        final String gcsUri = "gs://bottleneck-model-bucket/machine_data.jsonl";

        try {
            // Perform the data load operation from GCS to BigQuery
            loadDataFromGCS(datasetName, tableName, gcsUri);
        } catch (InterruptedException e) {
            System.err.println(getLocalName() + ": Interrupted while waiting for BigQuery job completion.");
            Thread.currentThread().interrupt(); // Restore interrupt status
        } catch (Exception e) {
            System.err.println(getLocalName() + ": Exception during BigQuery load operation.");
            e.printStackTrace();
        } finally {
            // Terminate the agent gracefully after operation is done
            doDelete();
        }
    }

    /**
     * Loads data from a Google Cloud Storage URI (JSONL format) into a specified BigQuery table.
     * The data will be appended to any existing data in the table.
     *
     * @param datasetName The BigQuery dataset name
     * @param tableName   The BigQuery table name
     * @param gcsUri      The Google Cloud Storage URI to the JSONL file
     * @throws InterruptedException If the thread is interrupted while waiting for the job completion
     */
    public void loadDataFromGCS(String datasetName, String tableName, String gcsUri) throws InterruptedException {
        // Construct the table identifier for BigQuery
        TableId tableId = TableId.of(datasetName, tableName);

        // Configure the load job with JSON format and append mode
        LoadJobConfiguration loadConfig = LoadJobConfiguration.newBuilder(tableId, gcsUri)
                .setFormatOptions(FormatOptions.json())  // JSON Lines format input
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)  // Append data, don't overwrite
                .build();

        System.out.println(getLocalName() + ": Submitting load job for GCS URI: " + gcsUri);

        // Create and submit the load job
        Job loadJob = bigQuery.create(JobInfo.of(loadConfig));

        // Block until job completes or thread is interrupted
        loadJob = loadJob.waitFor();

        // Handle job null scenario (e.g., job no longer exists)
        if (loadJob == null) {
            System.err.println(getLocalName() + ": Load job no longer exists.");
            return;
        }

        // Check job status for success or failure
        if (loadJob.isDone()) {
            if (loadJob.getStatus().getError() == null) {
                System.out.println(getLocalName() + ": Successfully loaded data from GCS to BigQuery.");
            } else {
                System.err.println(getLocalName() + ": BigQuery load job completed with errors: " + loadJob.getStatus().getError());
            }
        } else {
            System.err.println(getLocalName() + ": BigQuery load job did not complete successfully.");
        }
    }
}
