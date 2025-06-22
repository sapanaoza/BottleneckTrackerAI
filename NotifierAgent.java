package org.example.Agents;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

/**
 * NotifierAgent
 * --------------------
 * Listens for alert messages (especially bottlenecks) from other agents.
 * On receiving an alert, logs the machine ID, downtime, and timestamp.
 */
public class NotifierAgent extends Agent {

    @Override
    protected void setup() {
        log(" NotifierAgent Sends alerts and notifications based on detected bottlenecks....");

        // 1. Heartbeat behaviour: logs every 10 seconds to indicate agent is alive
        addBehaviour(new TickerBehaviour(this, 10_000) {
            @Override
            protected void onTick() {
                log(" is alive and waiting for alert messages...");
            }
        });

        // 2. CyclicBehaviour: continuously receives and processes incoming alert messages
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                ACLMessage msg = receive();  // Receive message if available

                if (msg != null) {
                    try {
                        String content = msg.getContent();
                        JsonObject json = JsonParser.parseString(content).getAsJsonObject();

                        // Check for expected alert type
                        if (json.has("type") && "bottleneck".equalsIgnoreCase(json.get("type").getAsString())) {
                            // Extract and log bottleneck alert details
                            String machineId = json.get("machineId").getAsString();
                            int downtime = json.get("downtime").getAsInt();
                            long timestamp = json.get("timestamp").getAsLong();

                            log(" Bottleneck Alert Received:" +
                                    "\n    Machine ID: " + machineId +
                                    "\n    Downtime: " + downtime + " minutes" +
                                    "\n    Timestamp: " + timestamp);
                        } else {
                            // Handle unexpected or unknown alert types
                            log(" Unknown alert type received: " + json.toString());
                        }

                    } catch (JsonSyntaxException e) {
                        // Handle JSON format issues
                        log(" Invalid JSON message received: " + msg.getContent());
                    } catch (Exception e) {
                        // Handle all other unexpected processing errors
                        log(" Error processing alert message: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    block();  // No message: block the thread to wait for the next one
                }
            }
        });
    }

    /**
     * Clean-up when agent is taken down from the platform.
     */
    @Override
    protected void takeDown() {
        log(" NotifierAgent stopped.");
    }

    /**
     * Utility method for consistent console logging with agent's local name.
     *
     * @param message The log message
     */
    private void log(String message) {
        System.out.println("[" + getLocalName() + "] " + message);
    }
}
