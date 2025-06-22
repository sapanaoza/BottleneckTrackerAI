package org.example.Agents;

import com.google.gson.*;
import jade.core.Agent;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class DataCollectorAgentADK extends Agent {

    // File paths via ENV
    private final String JSONL_PATH = System.getenv().getOrDefault("EXPORT_JSONL_PATH", "machine_data.jsonl");
    private final String CSV_PATH = System.getenv().getOrDefault("EXPORT_CSV_PATH", "machine_data.csv");



    private final List<JsonObject> receivedMessages = new CopyOnWriteArrayList<>();

    @Override
    protected void setup() {
        System.out.println(getLocalName() + " Starting data collection and analysis...");

        performTask();

        doDelete();
    }

    public void performTask() {
        try {
            JsonArray generatedData = new JsonArray();
            Map<String, List<Double>> runtimeMap = new HashMap<>();
            List<Map<String, Object>> analysisRows = new ArrayList<>();
            boolean shouldSendEmail = false;

            for (int i = 0; i < 10; i++) {
                String machineId = "M-" + (i % 3);
                double runtime = Math.random() * 150;

                JsonObject record = new JsonObject();
                record.addProperty("machineId", machineId);
                record.addProperty("timestamp", System.currentTimeMillis());
                record.addProperty("runtime", runtime);
                record.addProperty("status", (runtime > 100) ? "slow" : "normal");

                generatedData.add(record);
                runtimeMap.computeIfAbsent(machineId, k -> new ArrayList<>()).add(runtime);
            }

            for (Map.Entry<String, List<Double>> entry : runtimeMap.entrySet()) {
                String machineId = entry.getKey();
                List<Double> runtimes = entry.getValue();

                double avg = runtimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double max = runtimes.stream().mapToDouble(Double::doubleValue).max().orElse(1);
                double bottleneckRatio = avg / max;
                double bottleneckScore = avg / 100.0;
                boolean alert = bottleneckRatio > 0.8 || bottleneckScore > 1.0;

                if (alert) shouldSendEmail = true;

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

            writeJsonlFile(analysisRows, JSONL_PATH);

            if (!validateJsonlFile(JSONL_PATH)) {
                System.err.println(" JSONL validation failed. Aborting CSV export.");
                return;
            }

            convertJsonlToCsv(JSONL_PATH, CSV_PATH);



        } catch (Exception e) {
            System.err.println(" Error during data collection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void writeJsonlFile(List<Map<String, Object>> dataRows, String filePath) {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (Map<String, Object> row : dataRows) {
                writer.write(gson.toJson(row));
                writer.newLine();
            }
            System.out.println(" Exported JSONL file: " + filePath);
        } catch (IOException e) {
            System.err.println(" Failed to write JSONL: " + e.getMessage());
        }
    }

    public boolean validateJsonlFile(String jsonlFilePath) {
        Gson gson = new Gson();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(jsonlFilePath))) {
            String line;
            int lineNum = 1;
            while ((line = reader.readLine()) != null) {
                try {
                    gson.fromJson(line, JsonObject.class);
                } catch (JsonSyntaxException e) {
                    System.err.println(" Invalid JSON at line " + lineNum + ": " + e.getMessage());
                    return false;
                }
                lineNum++;
            }
        } catch (IOException e) {
            System.err.println(" Error reading file for validation: " + e.getMessage());
            return false;
        }
        return true;
    }

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

            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(csvFilePath))) {
                writer.write(String.join(",", headers));
                writer.newLine();
                for (Map<String, Object> row : rows) {
                    List<String> values = headers.stream()
                            .map(h -> {
                                String cell = String.valueOf(row.getOrDefault(h, ""));
                                return cell.contains(",") || cell.contains("\"") ? "\"" + cell.replace("\"", "\"\"") + "\"" : cell;
                            })
                            .collect(Collectors.toList());
                    writer.write(String.join(",", values));
                    writer.newLine();
                }
            }

            System.out.println(" Converted to CSV: " + csvFilePath);

        } catch (IOException e) {
            System.err.println(" Error converting JSONL to CSV: " + e.getMessage());
        }
    }



    public void receiveMessage(String message) {
        try {
            JsonObject json = JsonParser.parseString(message).getAsJsonObject();
            receivedMessages.add(json);
            System.out.println(getLocalName() + " received message: " + json);
        } catch (Exception e) {
            System.err.println(" Failed to parse incoming message: " + e.getMessage());
        }
    }

    public List<JsonObject> consumeReceivedMessages() {
        List<JsonObject> snapshot = new ArrayList<>(receivedMessages);
        receivedMessages.clear();
        return snapshot;
    }
}
