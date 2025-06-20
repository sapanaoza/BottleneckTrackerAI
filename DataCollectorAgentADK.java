package org.example.Agents;

import org.example.common.Agent;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;

public class DataCollectorAgentADK extends Agent {
    private final String projectId;
    private final String topicId;

    public DataCollectorAgentADK(String name, String projectId, String topicId) {
        super(name);
        this.projectId = projectId;
        this.topicId = topicId;
    }

    @Override
    public void performTask() {
        System.out.println(getName() + " starting data publishing...");

        try {
            TopicName topicName = TopicName.of(projectId, topicId);
            Publisher publisher = Publisher.newBuilder(topicName).build();

            for (int i = 0; i < 10; i++) {
                JsonObject jsonData = new JsonObject();
                jsonData.addProperty("machineId", "M-" + (i % 3));
                jsonData.addProperty("timestamp", System.currentTimeMillis());
                jsonData.addProperty("runtime", Math.random() * 100);  // Simulate runtime
                jsonData.addProperty("status", (i % 3 == 0) ? "slow" : "normal");

                ByteString data = ByteString.copyFromUtf8(jsonData.toString());

                PubsubMessage message = PubsubMessage.newBuilder()
                        .setData(data)
                        .putAttributes("id", UUID.randomUUID().toString())
                        .build();

                publisher.publish(message);
                System.out.println(getName() + " published: " + jsonData);

                Thread.sleep(1000); // Simulate time between messages
            }

            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);

        } catch (IOException | InterruptedException e) {
            System.err.println(getName() + " encountered error: " + e.getMessage());
        }

        System.out.println(getName() + " finished publishing.");
    }

    @Override
    public void receiveMessage(String message) {

    }
}
