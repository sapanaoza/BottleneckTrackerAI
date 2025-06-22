package org.example.Agents;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

/**
 * ProcessorAgent
 * --------------------
 * A JADE agent responsible for:
 * - Receiving machine telemetry messages in JSON format
 * - Detecting bottlenecks based on predefined criteria (downtime > threshold)
 * - Forwarding alerts to NotifierAgent
 */
public class ProcessorAgent extends Agent {

    // Threshold (in minutes) beyond which a machine is considered in bottleneck
    private static final int BOTTLENECK_THRESHOLD_DOWNTIME = 20;

    // Reusable Gson instance for JSON parsing
    private final Gson gson = new Gson();

    @Override
    protected void setup() {
        log("ProcessorAgent Orchestrates data processing, analysis, and inter-agent communication....");

        // 1. TickerBehaviour logs heartbeat every 10 seconds
        addBehaviour(new TickerBehaviour(this, 10_000) {
            @Override
            protected void onTick() {
                log(" is alive and awaiting machine data...");
            }
        });

        // 2. CyclicBehaviour to continuously listen and process messages
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                ACLMessage msg = receive();
                if (msg != null) {
                    log(" Message received from: " + msg.getSender().getLocalName());
                    try {
                        // Parse incoming JSON string into a MachineData object
                        JsonObject jsonObject = JsonParser.parseString(msg.getContent()).getAsJsonObject();
                        MachineData data = gson.fromJson(jsonObject, MachineData.class);

                        log(" Parsed machine data: " + jsonObject);

                        // Determine if bottleneck condition is met
                        if (isBottleneck(data)) {
                            log(" Bottleneck detected on machine: " + data.machineId);
                            sendAlert(data);  // Trigger alert to NotifierAgent
                        }

                    } catch (JsonSyntaxException e) {
                        log(" Invalid JSON received: " + e.getMessage());
                    } catch (Exception e) {
                        log(" Error while processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    block();  // Block until new message arrives
                }
            }
        });
    }

    /**
     * Clean-up actions when agent is terminated.
     */
    @Override
    protected void takeDown() {
        log(" ProcessorAgent stopped.");
    }

    /**
     * Checks if the machine data indicates a bottleneck condition.
     *
     * @param data Parsed machine data
     * @return true if downtime exceeds threshold
     */
    private boolean isBottleneck(MachineData data) {
        return data.downtime > BOTTLENECK_THRESHOLD_DOWNTIME;
    }

    /**
     * Sends a bottleneck alert to the NotifierAgent.
     *
     * @param data Machine data containing bottleneck info
     */
    private void sendAlert(MachineData data) {
        ACLMessage alertMsg = new ACLMessage(ACLMessage.INFORM);
        alertMsg.addReceiver(getAID("NotifierAgent"));  // Target agent must exist in the platform

        // Create alert JSON message
        JsonObject alert = new JsonObject();
        alert.addProperty("type", "bottleneck");
        alert.addProperty("machineId", data.machineId);
        alert.addProperty("downtime", data.downtime);
        alert.addProperty("timestamp", data.timestamp);

        alertMsg.setContent(alert.toString());
        send(alertMsg);

        log("📤 Alert sent to NotifierAgent: " + alert);
    }

    /**
     * Logs output with consistent agent name formatting.
     *
     * @param msg Message to log
     */
    private void log(String msg) {
        System.out.println("[" + getLocalName() + "] " + msg);
    }

    /**
     * POJO representing expected machine telemetry structure.
     */
    private static class MachineData {
        String machineId;
        long timestamp;
        int runTime;
        int downtime;
        int productionCount;
    }
}
