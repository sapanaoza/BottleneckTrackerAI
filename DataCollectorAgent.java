package org.example.Agents;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class DataCollectorAgent {
    private final String agentName;
    private final Publisher publisher;
    private final String resourcePath;

    public DataCollectorAgent(String agentName, Publisher publisher, String resourcePath) {
        this.agentName = agentName;
        this.publisher = publisher;
        this.resourcePath = resourcePath;
    }

    public void performTask() {
        System.out.println(agentName + " started collecting machine data...");

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                System.err.println("Resource not found: " + resourcePath);
                return;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                boolean isFirstLine = true;

                while ((line = reader.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false; // Skip CSV header
                        continue;
                    }

                    System.out.println("Read line: " + line);
                    String[] tokens = line.split(",");
                    if (tokens.length != 4) {
                        System.err.println("Skipping malformed line: " + line);
                        continue;
                    }

                    String machineId = tokens[0].trim();
                    long timestamp;
                    double runtime;
                    String status = tokens[3].trim();

                    try {
                        timestamp = Long.parseLong(tokens[1].trim());
                        runtime = Double.parseDouble(tokens[2].trim());
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping line with invalid number format: " + line);
                        continue;
                    }

                    String jsonPayload = String.format(
                            "{\"machineId\":\"%s\",\"timestamp\":%d,\"runtime\":%.2f,\"status\":\"%s\"}",
                            machineId, timestamp, runtime, status
                    );

                    System.out.printf("Machine: %s, Runtime: %.2f, Status: %s%n", machineId, runtime, status);
                    System.out.println("Publishing message: " + jsonPayload);

                    PubsubMessage message = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(jsonPayload))
                            .build();

                    // Publish asynchronously with simple callback
                    publisher.publish(message).addListener(() ->
                                    System.out.println("Message published for machine " + machineId),
                            Runnable::run
                    );
                }
            }
        } catch (Exception e) {
            System.err.println("Error in DataCollectorAgent: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
