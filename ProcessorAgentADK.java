
package org.example.Agents;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import com.google.gson.Gson;

public class ProcessorAgentADK extends Agent {

    private static final int BOTTLENECK_THRESHOLD_DOWNTIME = 20;
    private Gson gson = new Gson();

    @Override
    protected void setup() {
        System.out.println(getLocalName() + ": ProcessorAgent started.");

        // Heartbeat every 10 seconds
        addBehaviour(new TickerBehaviour(this, 10000) {
            @Override
            protected void onTick() {
                System.out.println(getLocalName() + " is alive and waiting for machine data...");
            }
        });

        // Message receiver behaviour
        addBehaviour(new CyclicBehaviour(this) {
            @Override
            public void action() {
                ACLMessage msg = receive();
                if (msg != null) {
                    String json = msg.getContent();
                    String fixedJson = json.replaceAll("(\\w+)\\s*:", "\"$1\":")
                            .replaceAll(":([^\",\\}\\d][^,\\}]*)", ":\"$1\"");

                    JsonObject jsonObject = JsonParser.parseString(fixedJson).getAsJsonObject();

                    String machineId = jsonObject.get("machineId").getAsString();
                    long timestamp = jsonObject.get("timestamp").getAsLong();
                    int runTime = jsonObject.get("runTime").getAsInt();
                    int downtime = jsonObject.get("downtime").getAsInt();
                    int productionCount = jsonObject.get("productionCount").getAsInt();

                    System.out.println(getLocalName() + " received message: " + jsonObject);

                    try {
                        MachineData data = gson.fromJson(jsonObject, MachineData.class);

                        if (data.downtime > BOTTLENECK_THRESHOLD_DOWNTIME) {
                            System.out.println("Bottleneck detected on machine " + data.machineId);

                            ACLMessage alertMsg = new ACLMessage(ACLMessage.INFORM);
                            alertMsg.addReceiver(getAID("NotifierAgent"));  // Target agent by name
                            alertMsg.setContent("Bottleneck detected on machine " + data.machineId + " with downtime " + data.downtime);
                            send(alertMsg);
                        }
                    } catch (Exception e) {
                        System.err.println(getLocalName() + ": Failed to parse machine data JSON - " + e.getMessage());
                    }
                } else {
                    block();
                }
            }
        });
    }

    @Override
    protected void takeDown() {
        System.out.println(getLocalName() + ": ProcessorAgent shutting down.");
    }

    // Inner class representing machine data
    private static class MachineData {
        String machineId;
        long timestamp;
        int runTime;
        int downtime;
        int productionCount;
    }
}