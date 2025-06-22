package org.example.Agents;

import com.google.gson.*;
import jade.core.Agent;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * DataCollectorAgentADK simulates machine runtime data,
 * performs statistical analysis to detect bottlenecks,
 * and exports data in both JSONL and CSV formats.
 *
 * This agent can be extended to collect real-time data
 * or receive messages via JADE or external connectors.
 */
public class DataCollectorAgentADK extends Agent {

    // File paths can be overridden via environment variables
    private final String JSONL_PATH = System.getenv().getOrDefault("EXPORT_JSONL_PATH", "machine_data.jsonl");
    private final String CSV_PATH = System.getenv().getOrDefault("EXPORT_CSV_PATH", "machine_data.csv");

    // Thread-safe list to collect received messages (if any)
    private final List<JsonObject> receivedMessages = new CopyOnWriteArrayList<>();

    @Override
    protected void setup() {
        System.out.println(getLocalName() + " DataCollectorAgent Beginning data collection and analysis...");

        performTask();

        // Terminate the agent after one-time execution
        doDelete();
    }

    /**
     * Simulates machine data, analyzes it, and exports to JSONL and CSV.
     */
    public void performTask() {
        try {
            JsonArray generatedData = new JsonArray(); // For debug or future use
            Map<String, List<Double>> runtimeMap = new HashMap<>(); // Map of machineId to runtimes
            List<Map<String, Object>> analysisRows = new ArrayList<>(); // Output records for export

            // Simulate data for 10 samples
            for (int i = 0; i < 10; i++) {
                String machineId = "M-" + (i % 3); // Create 3 distinct machines
                double runtime = Math.random() * 150;

                JsonObject record = new JsonObject();
                record.addProperty("machineId", machineId);
                record.addProperty("timestamp", System.currentTimeMillis());
                record.addProperty("runtime", runtime);
                record.addProperty("status", (runtime > 100) ? "slow" : "normal");

                generatedData.add(record);
                runtimeMap.computeIfAbsent(machineId, k -> new ArrayList<>()).add(runtime);
            }

            // Analyze each machine's runtimes
            for (Map.Entry<String, List<Double>> entry : runtimeMap.entrySet()) {
                String machineId = entry.getKey();
                List<Double> runtimes = entry.getValue();

                double avg = runtimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double max = runtimes.stream().mapToDouble(Double::doubleValue).max().orElse(1);
                double bottleneckRatio = avg / max;
                double bottleneckScore = avg / 100.0;
                boolean alert = bottleneckRatio > 0.8 || bottleneckScore > 1.0;

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("machineId", machineId);
                row.put("avgRuntime", avg);
                row.put("maxRuntime", max);
                row.put("bottleneckRatio", bottleneckRatio);
                row.put("bottleneckScore", bottleneckScore);
                row.put("alert", alert);
                row.put("timestamp", System.currentTimeMillis());

                analysisRows.add(row);
            }

            // Export results
            writeJsonlFile(analysisRows, JSONL_PATH);
            convertJsonlToCsv(JSONL_PATH, CSV_PATH);

        } catch (Exception e) {
            System.err.println(getLocalName() + "  Error during data collection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Writes a list of data rows to a JSONL (.jsonl) file.
     *
     * @param dataRows  List of maps representing each row
     * @param filePath  Path to save the file
     */
    public void writeJsonlFile(List<Map<String, Object>> dataRows, String filePath) {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            Gson gson = new Gson();
            for (Map<String, Object> row : dataRows) {
                writer.write(gson.toJson(row));
                writer.newLine();
            }
            System.out.println(" Exported JSONL file: " + filePath);
        } catch (IOException e) {
            System.err.println(" Failed to write JSONL: " + e.getMessage());
        }
    }

    /**
     * Converts a JSONL file to CSV format.
     *
     * @param jsonlFilePath  Input JSONL file path
     * @param csvFilePath    Output CSV file path
     */
    public void convertJsonlToCsv(String jsonlFilePath, String csvFilePath) {
        Gson gson = new Gson();
        List<String> headers = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonlFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonObject json = gson.fromJson(line, JsonObject.class);
                Map<String, Object> row = new LinkedHashMap<>();

                for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue().isJsonNull() ? "" : entry.getValue().getAsString();
                    row.put(key, value);
                    if (!headers.contains(key)) headers.add(key);
                }
                rows.add(row);
            }

            // Write CSV
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(csvFilePath))) {
                writer.write(String.join(",", headers));
                writer.newLine();
                for (Map<String, Object> row : rows) {
                    List<String> values = new ArrayList<>();
                    for (String header : headers) {
                        values.add(String.valueOf(row.getOrDefault(header, "")));
                    }
                    writer.write(String.join(",", values));
                    writer.newLine();
                }
            }

            System.out.println(" Converted to CSV: " + csvFilePath);

        } catch (IOException e) {
            System.err.println(" Error converting JSONL to CSV: " + e.getMessage());
        }
    }

    /**
     * Receives a JSON message and stores it in memory.
     *
     * @param message  Incoming message as JSON string
     */
    public void receiveMessage(String message) {
        try {
            JsonObject json = JsonParser.parseString(message).getAsJsonObject();
            receivedMessages.add(json);
            System.out.println(getLocalName() + "  received message: " + json);
        } catch (Exception e) {
            System.err.println(" Failed to parse incoming message: " + e.getMessage());
        }
    }

    /**
     * Returns and clears all collected messages.
     *
     * @return List of parsed messages
     */
    public List<JsonObject> consumeReceivedMessages() {
        List<JsonObject> snapshot = new ArrayList<>(receivedMessages);
        receivedMessages.clear();
        return snapshot;
    }
}
