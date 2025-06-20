package org.example.Agents;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

public class NotifierAgent extends Agent {

    @Override
    protected void setup() {
        // Log agent startup
        log("NotifierAgent starting...");

        // 1. Heartbeat behaviour: logs every 10 seconds that agent is alive and waiting for messages
        addBehaviour(new TickerBehaviour(this, 10000) {
            @Override
            protected void onTick() {
                log("is alive and waiting for messages...");
            }
        });

        // 2. CyclicBehaviour: continuously receives and processes incoming messages
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                // Try to receive a message from the message queue
                ACLMessage msg = receive();
                if (msg != null) {
                    try {
                        String content = msg.getContent();

                        // Parse message content as JSON
                        JsonObject json = JsonParser.parseString(content).getAsJsonObject();

                        // Check if the JSON contains a bottleneck alert type
                        if (json.has("type") && json.get("type").getAsString().equalsIgnoreCase("bottleneck")) {
                            // Extract relevant alert details
                            String machineId = json.get("machineId").getAsString();
                            int downtime = json.get("downtime").getAsInt();
                            long timestamp = json.get("timestamp").getAsLong();

                            // Log the received bottleneck alert details
                            log(" Bottleneck Alert received:\n" +
                                    "    Machine ID: " + machineId + "\n" +
                                    "    Downtime: " + downtime + " mins\n" +
                                    "    Timestamp: " + timestamp);
                        } else {
                            // Received message is not a bottleneck alert; log the unknown alert type
                            log("Received unknown alert type: " + json.toString());
                        }

                    } catch (JsonSyntaxException e) {
                        // Handle messages that are not valid JSON
                        log("Received non-JSON or invalid message: " + msg.getContent());
                    } catch (Exception e) {
                        // Handle any other unexpected errors during message processing
                        log("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    // No message was received; block and wait to be notified of new messages
                    block();
                }
            }
        });
    }

    @Override
    protected void takeDown() {
        // Cleanup and logging on agent shutdown
        log("NotifierAgent stopped.");
    }

    // Helper method for consistent console logging with agent name prefix
    private void log(String message) {
        System.out.println("[" + getLocalName() + "] " + message);
    }
}
