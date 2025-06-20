package org.example.Agents;

import com.google.cloud.pubsub.v1.Publisher;
import org.example.common.Agent;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class BottleneckDetectorAgent extends Agent {
    private final String subscriptionId;
    private final Publisher alertPublisher;

    public BottleneckDetectorAgent(String name, String subscriptionId, Publisher alertPublisher) {
        super(name);
        this.subscriptionId = subscriptionId;
        this.alertPublisher = alertPublisher;
    }

    @Override
    public void performTask() {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of("multi-agent-hackathon", subscriptionId);

        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, (PubsubMessage message, AckReplyConsumer consumer) -> {
            String jsonStr = message.getData().toStringUtf8();
            JsonObject json = JsonParser.parseString(jsonStr).getAsJsonObject();


            int runtime = json.has("runtime") ? json.get("runtime").getAsInt() : 0;
            int downtime = json.has("downtime") ? json.get("downtime").getAsInt() : 0;

            if (downtime > 20 || runtime < 30) {
                JsonObject alert = new JsonObject();
                alert.addProperty("machineId", json.get("machineId").getAsString());
                alert.addProperty("timestamp", json.get("timestamp").getAsLong());
                alert.addProperty("reason", "Potential bottleneck");

                ByteString data = ByteString.copyFromUtf8(alert.toString());
                PubsubMessage alertMessage = PubsubMessage.newBuilder().setData(data).build();
                alertPublisher.publish(alertMessage);
            }

            consumer.ack();
        }).build();

        subscriber.startAsync().awaitRunning();
    }

    @Override
    public void receiveMessage(String message) {

    }
}