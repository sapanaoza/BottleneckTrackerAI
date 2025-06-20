package org.example.Agents;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.example.common.Agent;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataCollectorAgentsADK extends Agent {
    // File paths for storing processed data locally
    private final String filePath = "C:\\Users\\Falgun\\OneDrive\\Desktop\\Personal\\Java\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.jsonl"; // JSONL output file
    private final String csvFilePath = "C:\\Users\\Falgun\\OneDrive\\Desktop\\Personal\\Java\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.csv"; // CSV output file

    // Thread-safe list to collect received Pub/Sub messages as JSON objects
    private final List<JsonObject> receivedMessages = new CopyOnWriteArrayList<>();

    // Constructor - calls superclass Agent constructor with agent name
    public DataCollectorAgentsADK(String name) {
        super(name);
    }

    /**
     * Main task performed by the agent.
     * Simulates collection and analysis of machine runtime data,
     * then exports results locally as JSONL and CSV files.
     */
    @Override
    public void performTask() {
        System.out.println(getName() + " starting bottleneck data analysis and local export...");

        try {
            JsonArray instancesArray = new JsonArray();  // Store simulated raw data instances
            Map<String, List<Double>> machineRuntimeMap = new HashMap<>();  // Map machineId -> list of runtimes
            List<Map<String, Object>> dataRows = new ArrayList<>();  // Rows for analysis results

            // Simulate generating machine runtime data for 10 samples
            for (int i = 0; i < 10; i++) {
                String machineId = "M-" + (i % 3);  // Simulated machine IDs cycling through M-0, M-1, M-2
                double runtime = Math.random() * 150;  // Random runtime between 0 and 150

                // Create JSON object for this sample data
                JsonObject jsonData = new JsonObject();
                jsonData.addProperty("machineId", machineId);
                jsonData.addProperty("timestamp", System.currentTimeMillis());
                jsonData.addProperty("runtime", runtime);
                jsonData.addProperty("status", (runtime > 100) ? "slow" : "normal"); // simple status logic

                instancesArray.add(jsonData);

                // Collect runtime for each machine to compute statistics later
                machineRuntimeMap.computeIfAbsent(machineId, k -> new ArrayList<>()).add(runtime);
            }

            // Analyze each machine's collected runtimes to detect potential bottlenecks
            for (Map.Entry<String, List<Double>> entry : machineRuntimeMap.entrySet()) {
                String machineId = entry.getKey();
                List<Double> runtimes = entry.getValue();

                // Calculate average and maximum runtime
                double avg = runtimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double max = runtimes.stream().mapToDouble(Double::doubleValue).max().orElse(1);

                // Bottleneck ratio is average/max runtime
                double bottleneckRatio = avg / max;

                // Bottleneck score based on average runtime threshold (scaled)
                double bottleneckScore = avg / 100.0;

                // Condition to decide if machine requires attention
                boolean requiresAttention = bottleneckRatio > 0.8 || bottleneckScore > 1.0;

                // Prepare a data row representing analyzed info for output
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("machineId", machineId);
                row.put("avgRuntime", avg);
                row.put("maxRuntime", max);
                row.put("bottleneckRatio", bottleneckRatio);
                row.put("bottleneckScore", bottleneckScore);
                row.put("alert", requiresAttention);
                row.put("timestamp", System.currentTimeMillis());

                dataRows.add(row);
            }

            // Write analyzed results as JSONL to local file
            writeToLocalFile(dataRows, filePath);

            // Convert the JSONL file to CSV for easier consumption and reporting
            convertJsonlToCsv(filePath, csvFilePath);

        } catch (Exception e) {
            System.err.println(getName() + " error during data processing: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Writes a list of data rows as JSON objects, one per line, to a local file.
     *
     * @param dataRows List of rows (maps) to write
     * @param filePath Destination file path
     */
    public void writeToLocalFile(List<Map<String, Object>> dataRows, String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            Gson gson = new Gson();
            for (Map<String, Object> row : dataRows) {
                writer.write(gson.toJson(row));  // Serialize each map as JSON
                writer.newLine();
            }
            System.out.println(" Data exported to local file: " + filePath);
        } catch (IOException e) {
            System.err.println(" Failed to write to file: " + e.getMessage());
        }
    }

    /**
     * Reads a JSONL file and converts it into CSV format.
     *
     * @param jsonlFilePath Input JSONL file path
     * @param csvFilePath   Output CSV file path
     */
    public static void convertJsonlToCsv(String jsonlFilePath, String csvFilePath) {
        Gson gson = new Gson();
        List<String> headers = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(jsonlFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject json = gson.fromJson(line, JsonObject.class);
                Map<String, Object> row = new LinkedHashMap<>();

                // Extract all fields, track headers dynamically
                for (Map.Entry<String, com.google.gson.JsonElement> entry : json.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue().isJsonNull() ? "" : entry.getValue().getAsString();

                    row.put(key, value);
                    if (!headers.contains(key)) {
                        headers.add(key);
                    }
                }
                rows.add(row);
            }

            // Write CSV file with header row and data rows
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {
                // Write header line
                writer.write(String.join(",", headers));
                writer.newLine();

                // Write data lines in the order of headers
                for (Map<String, Object> row : rows) {
                    List<String> values = new ArrayList<>();
                    for (String header : headers) {
                        Object val = row.getOrDefault(header, "");
                        values.add(val.toString());
                    }
                    writer.write(String.join(",", values));
                    writer.newLine();
                }
            }

            System.out.println(" Converted JSONL to CSV: " + csvFilePath);

        } catch (IOException e) {
            System.err.println(" Error during conversion: " + e.getMessage());
        }
    }

    /**
     * Method to receive and store incoming messages (e.g., from Pub/Sub subscriber).
     *
     * @param message Incoming JSON message as string
     */
    @Override
    public void receiveMessage(String message) {
        System.out.println(getName() + " received message: " + message);
        try {
            JsonObject json = JsonParser.parseString(message).getAsJsonObject();
            receivedMessages.add(json);
        } catch (Exception e) {
            System.err.println(getName() + " - failed to parse message: " + e.getMessage());
        }
    }

    /**
     * Method to safely retrieve and clear all received messages.
     *
     * @return List of JsonObject messages received so far
     */
    public List<JsonObject> consumeReceivedMessages() {
        List<JsonObject> snapshot = new ArrayList<>(receivedMessages);
        receivedMessages.clear();
        return snapshot;
    }
}
