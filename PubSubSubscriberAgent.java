package org.example.Agents;

import jade.core.Agent;
import com.google.api.core.ApiService;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.util.concurrent.TimeoutException;

/**
 * PubSubSubscriberAgent
 * ----------------------
 * A JADE agent that subscribes to a Google Cloud Pub/Sub subscription and processes messages.
 * The agent runs the subscriber asynchronously in a background thread.
 *
 * Expected setup:
 * - Environment variable GOOGLE_APPLICATION_CREDENTIALS must be set to the service account key file.
 * - Pub/Sub topic and subscription should already exist.
 */
public class PubSubSubscriberAgent extends Agent {

    // Set your GCP project ID and Pub/Sub subscription ID
    private final String projectId = "multi-agent-hackathon";
    private final String subscriptionId = "machine-metrics-sub";

    private Subscriber subscriber; // Reference for shutdown if needed

    @Override
    protected void setup() {
        System.out.println(getLocalName() + " started. Initializing Pub/Sub subscription...");

        // Add JVM shutdown hook to close subscriber gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered: cleaning up...");
            if (subscriber != null) {
                subscriber.stopAsync();
                try {
                    subscriber.awaitTerminated(1, java.util.concurrent.TimeUnit.MINUTES);
                    System.out.println("Subscriber terminated successfully.");
                } catch (java.util.concurrent.TimeoutException e) {
                    System.err.println("Timeout waiting for subscriber termination: " + e.getMessage());
                }
            }
        }));

        // Launch the subscriber in a separate thread to avoid blocking JADE thread
        new Thread(() -> subscribeToPubSub(projectId, subscriptionId)).start();
    }

    /**
     * Initializes and starts a Google Pub/Sub subscriber.
     *
     * @param projectId       GCP project ID
     * @param subscriptionId  Pub/Sub subscription ID (must already exist)
     */
    private void subscribeToPubSub(String projectId, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        // Define message receiver logic
        subscriber = Subscriber.newBuilder(subscriptionName, (PubsubMessage message, AckReplyConsumer consumer) -> {
            try {
                String messageData = message.getData().toStringUtf8();
                System.out.printf("%s received message: %s%n", getLocalName(), messageData);

                // ✅ TODO: Add custom message processing logic here
                // You can trigger internal agent behaviors, notify other agents, or log to file

                consumer.ack(); // ✅ Always acknowledge to avoid redelivery
            } catch (Exception e) {
                System.err.printf(" Error processing message: %s%n", e.getMessage());
                consumer.nack(); // ❌ NACK to requeue the message if needed
            }
        }).build();

        // Attach listener to handle subscriber state and failure
        subscriber.addListener(new Subscriber.Listener() {
            @Override
            public void failed(State from, Throwable failure) {
                System.err.printf(" PubSub Subscriber failed in state %s: %s%n", from, failure.getMessage());
                failure.printStackTrace();
            }
        }, MoreExecutors.directExecutor());

        // Start the subscriber
        try {
            subscriber.startAsync().awaitRunning();
            System.out.printf(" %s: Pub/Sub subscriber started for [%s]%n", getLocalName(), subscriptionId);
        } catch (IllegalStateException e) {
            System.err.println(" Failed to start subscriber: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Clean up resources when the agent is killed.
     */
    @Override
    protected void takeDown() {
        if (subscriber != null) {
            System.out.println("Shutting down Pub/Sub subscriber...");
            subscriber.stopAsync();
            try {
                // Wait up to 1 minute for shutdown to complete
                subscriber.awaitTerminated(1, java.util.concurrent.TimeUnit.MINUTES);
            } catch (java.util.concurrent.TimeoutException e) {
                System.err.println("Timeout waiting for subscriber termination");
            } catch (IllegalStateException e) {
                System.err.println("Subscriber did not terminate properly: " + e.getMessage());
            }
        }
        System.out.println(getLocalName() + " terminated.");
    }

}
