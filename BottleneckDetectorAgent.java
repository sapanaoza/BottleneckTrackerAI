package org.example.Agents;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.*;
import com.google.gson.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import jade.core.Agent;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * BottleneckDetectorAgent listens to a Google Cloud Pub/Sub subscription,
 * processes machine metrics (runtime, downtime),
 * applies threshold-based bottleneck detection logic,
 * and publishes alerts to a Pub/Sub topic for downstream systems.
 *
 * This agent is part of a multi-agent system using JADE and integrates tightly with GCP Pub/Sub.
 */
public class BottleneckDetectorAgent extends Agent {

    // Logger for diagnostic and operational messages
    private static final Logger logger = Logger.getLogger(BottleneckDetectorAgent.class.getName());

    // GSON for JSON parsing
    private static final Gson gson = new Gson();

    // Project and Pub/Sub resource identifiers
    private static final String PROJECT_ID = "multi-agent-hackathon";
    private static final String SUBSCRIPTION_ID = "machine-metrics-sub";
    private static final String ALERT_TOPIC_ID = "bottleneck-alerts-topic";

    // Thresholds for bottleneck detection logic
    private static final int MAX_DOWNTIME = 20; // in minutes
    private static final int MIN_RUNTIME = 30;  // in minutes

    // Pub/Sub subscriber instance
    private transient Subscriber subscriber;

    /**
     * JADE agent setup lifecycle method.
     * Initializes Pub/Sub subscriber, defines processing logic, and handles alert publishing.
     */
    @Override
    protected void setup() {
        logger.info(getLocalName() + "Simulates Google Cloud Pub/Sub messaging for real-time agent communication....");

        // Define the subscription and alert topic
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);
        ProjectTopicName alertTopic = ProjectTopicName.of(PROJECT_ID, ALERT_TOPIC_ID);

        // Publisher for sending alerts
        Publisher alertPublisher;
        try {
            alertPublisher = Publisher.newBuilder(alertTopic).build();
        } catch (IOException e) {
            logger.severe(" Failed to create Pub/Sub alert publisher: " + e.getMessage());
            doDelete();
            return;
        }

        // Message processing logic
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            String payload = message.getData().toStringUtf8();
            logger.info(" Received message: " + payload);

            try {
                JsonObject json = gson.fromJson(payload, JsonObject.class);
                String machineId = json.get("machineId").getAsString();
                int runtime = json.get("runtime").getAsInt();
                int downtime = json.get("downtime").getAsInt();
                long timestamp = json.get("timestamp").getAsLong();

                // Bottleneck logic: too much downtime or not enough runtime
                if (downtime > MAX_DOWNTIME || runtime < MIN_RUNTIME) {
                    JsonObject alert = new JsonObject();
                    alert.addProperty("machineId", machineId);
                    alert.addProperty("timestamp", timestamp);
                    alert.addProperty("reason", "Potential bottleneck detected");

                    PubsubMessage alertMessage = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(alert.toString()))
                            .build();

                    alertPublisher.publish(alertMessage);  // Non-blocking publish
                    logger.warning(" Bottleneck detected! Alert sent for machine: " + machineId);
                } else {
                    logger.info(" Machine " + machineId + " operating within thresholds.");
                }

            } catch (JsonSyntaxException | NullPointerException ex) {
                logger.severe(" Invalid JSON or missing fields: " + ex.getMessage());
            } finally {
                consumer.ack();  // Acknowledge message to Pub/Sub
            }
        };

        // Subscriber configuration
        subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setParallelPullCount(1)  // Single-threaded for ordered processing
                .build();

        // Add error listener
        subscriber.addListener(new Subscriber.Listener() {
            public void failed(ApiService.State from, Throwable failure) {
                logger.severe(" Subscriber error: " + failure.getMessage());
            }
        }, Runnable::run);

        // Start subscriber
        subscriber.startAsync().awaitRunning();
        logger.info(getLocalName() + "  Now listening for machine metrics...");

        // Optional: self-terminate after 5 minutes (can be adjusted or made permanent)
        addBehaviour(new jade.core.behaviours.OneShotBehaviour() {
            public void action() {
                try {
                    TimeUnit.MINUTES.sleep(5);  // Run time for demonstration/testing
                    subscriber.stopAsync();
                    logger.info(" " + getLocalName() + " stopped after 5-minute window.");
                } catch (InterruptedException e) {
                    logger.warning(" Agent interrupted: " + e.getMessage());
                } finally {
                    doDelete();  // Clean shutdown
                }
            }
        });
    }
}
