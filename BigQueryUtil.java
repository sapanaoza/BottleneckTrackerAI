package org.example.Agents;

import com.google.cloud.bigquery.*;
import jade.core.Agent;

/**
 * JADE Agent to manage BigQuery setup:
 * - Creates a BigQuery dataset if it does not exist
 * - Creates a BigQuery table with a predefined schema if it does not exist
 *
 * Expects exactly 3 arguments on startup:
 *   args[0] = GCP Project ID
 *   args[1] = Dataset name
 *   args[2] = Table name
 */
public class BigQueryUtil extends Agent {

    /**
     * Default constructor.
     */
    public BigQueryUtil() {
        super();
    }

    /**
     * JADE agent lifecycle method called upon agent start.
     * Validates arguments, then creates dataset and table as needed.
     */
    @Override
    protected void setup() {
        System.out.println(getLocalName() + " Provides helper functions for interacting with BigQuery APIs...");

        Object[] args = getArguments();
        if (args == null || args.length < 3) {
            System.err.println(" Missing arguments. Usage: <projectId> <datasetName> <tableName>");
            doDelete();  // Terminate agent if arguments are invalid
            return;
        }

        String projectId = args[0].toString();
        String datasetName = args[1].toString();
        String tableName = args[2].toString();

        // Define the BigQuery table schema matching expected data structure
        Schema schema = Schema.of(
                Field.of("machineId", StandardSQLTypeName.STRING),
                Field.of("timestamp", StandardSQLTypeName.INT64),
                Field.of("avgRuntime", StandardSQLTypeName.FLOAT64),
                Field.of("maxRuntime", StandardSQLTypeName.FLOAT64),
                Field.of("bottleneckRatio", StandardSQLTypeName.FLOAT64),
                Field.of("bottleneckScore", StandardSQLTypeName.FLOAT64),
                Field.of("alert", StandardSQLTypeName.BOOL)
        );

        try {
            // Initialize BigQuery client with given project ID
            BigQuery bigquery = BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .build()
                    .getService();

            // Create dataset if it does not exist
            Dataset dataset = bigquery.getDataset(datasetName);
            if (dataset == null) {
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
                bigquery.create(datasetInfo);
                System.out.println(" Dataset created: " + datasetName);
            } else {
                System.out.println(" Dataset already exists: " + datasetName);
            }

            // Create table if it does not exist
            TableId tableId = TableId.of(datasetName, tableName);
            Table table = bigquery.getTable(tableId);
            if (table == null) {
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
                bigquery.create(tableInfo);
                System.out.println(" Table created: " + tableName);
            } else {
                System.out.println(" Table already exists: " + tableName);
            }

        } catch (BigQueryException bqe) {
            System.err.println(" BigQuery exception: " + bqe.getMessage());
        } catch (Exception e) {
            System.err.println(" Unexpected error during BigQuery setup:");
            e.printStackTrace();
        }

        System.out.println(getLocalName() + " completed BigQuery setup.");
        doDelete();  // Terminate agent gracefully after work is done
    }
}
