package org.example.Agents;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

public class ProcessorAgent extends Agent {

    // Threshold for downtime (in minutes) to consider as a bottleneck
    private static final int BOTTLENECK_THRESHOLD_DOWNTIME = 20;

    // Gson instance to parse JSON messages
    private final Gson gson = new Gson();

    @Override
    protected void setup() {
        log("ProcessorAgent starting...");

        // 1. Heartbeat behaviour to log agent status every 10 seconds
        addBehaviour(new TickerBehaviour(this, 10000) {
            @Override
            protected void onTick() {
                log("is alive and waiting for machine data...");
            }
        });

        // 2. Cyclic behaviour to process incoming messages continuously
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                // Check for new message
                ACLMessage msg = receive();
                if (msg != null) {
                    try {
                        // Parse message content as JSON
                        JsonObject jsonObject = JsonParser.parseString(msg.getContent()).getAsJsonObject();
                        MachineData data = gson.fromJson(jsonObject, MachineData.class);

                        log("Received machine data: " + jsonObject);

                        // Check if machine data indicates a bottleneck
                        if (isBottleneck(data)) {
                            log("Bottleneck detected on machine " + data.machineId);
                            sendAlert(data);  // Send alert to NotifierAgent
                        }

                    } catch (JsonSyntaxException e) {
                        // Handle invalid JSON format in message
                        log("Invalid JSON received: " + e.getMessage());
                    } catch (Exception e) {
                        // Handle any other exceptions during message processing
                        log("Error while processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    // No message available, block the behaviour until a new message arrives
                    block();
                }
            }
        });
    }

    @Override
    protected void takeDown() {
        // Cleanup or logging when agent is terminated
        log("ProcessorAgent stopped.");
    }

    // Check if machine downtime exceeds the bottleneck threshold
    private boolean isBottleneck(MachineData data) {
        return data.downtime > BOTTLENECK_THRESHOLD_DOWNTIME;
    }

    // Send alert message to NotifierAgent when a bottleneck is detected
    private void sendAlert(MachineData data) {
        ACLMessage alertMsg = new ACLMessage(ACLMessage.INFORM);
        alertMsg.addReceiver(getAID("NotifierAgent")); // Target agent name

        // Create alert content as JSON
        JsonObject alert = new JsonObject();
        alert.addProperty("type", "bottleneck");
        alert.addProperty("machineId", data.machineId);
        alert.addProperty("downtime", data.downtime);
        alert.addProperty("timestamp", data.timestamp);

        alertMsg.setContent(alert.toString());
        send(alertMsg); // Send the alert message
    }

    // Utility method for consistent logging with agent name prefix
    private void log(String msg) {
        System.out.println("[" + getLocalName() + "] " + msg);
    }

    // Inner class representing the machine data structure expected in messages
    private static class MachineData {
        String machineId;
        long timestamp;
        int runTime;
        int downtime;
        int productionCount;
    }
}
