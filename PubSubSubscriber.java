package org.example.Agents;

import com.google.api.core.ApiService.State;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import jade.core.Agent;

public class PubSubSubscriber extends Agent {

    // The Google Cloud Pub/Sub subscriber instance (transient since not serializable)
    private transient Subscriber subscriber;
    private String projectId;      // GCP Project ID
    private String subscriptionId; // Pub/Sub Subscription ID

    // Reference to your DataCollectorAgentsADK to forward messages for processing
    private DataCollectorAgentsADK dataCollectorAgent;

    // Default constructor required by JADE
    public PubSubSubscriber() {
    }

    // Setter to inject reference to DataCollectorAgentsADK
    public void setDataCollectorAgent(DataCollectorAgentsADK agent) {
        this.dataCollectorAgent = agent;
    }

    /**
     * Agent setup method called once when the agent is launched.
     * Retrieves arguments, starts subscriber in a separate thread.
     */
    @Override
    protected void setup() {
        try {
            // Retrieve arguments passed to the agent (expected: projectId, subscriptionId)
            Object[] args = getArguments();
            if (args != null && args.length >= 2) {
                projectId = (String) args[0];
                subscriptionId = (String) args[1];
            } else {
                System.err.println("Missing PubSub arguments. Usage: projectId, subscriptionId");
                doDelete(); // Terminates the agent if arguments missing
                return;
            }

            // Start subscriber asynchronously on a new thread to avoid blocking setup
            new Thread(() -> {
                try {
                    startSubscriber();
                } catch (Exception e) {
                    System.err.println(getLocalName() + " - Failed to start subscriber: " + e.getMessage());
                    e.printStackTrace();
                }
            }).start();

        } catch (Exception e) {
            System.err.println(getLocalName() + " - Setup failed: " + e.getMessage());
            e.printStackTrace();
            doDelete(); // Terminate agent on setup failure
        }
    }

    /**
     * Starts the Pub/Sub subscriber which listens for messages on the specified subscription.
     */
    private void startSubscriber() {
        try {
            // Create subscription name object using project and subscription IDs
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

            // Build subscriber with a message receiver callback lambda
            subscriber = Subscriber.newBuilder(subscriptionName, (PubsubMessage message, AckReplyConsumer consumer) -> {
                try {
                    // Extract message data as UTF-8 string
                    String content = message.getData().toStringUtf8();
                    System.out.println(getLocalName() + " received message: " + content);

                    // Forward the message content to the dataCollectorAgent if available
                    if (dataCollectorAgent != null) {
                        dataCollectorAgent.receiveMessage(content);
                    }

                    // Acknowledge the message to Pub/Sub
                    consumer.ack();
                } catch (Exception e) {
                    System.err.println(getLocalName() + " - Error processing message: " + e.getMessage());
                    e.printStackTrace();
                    // Negative acknowledge the message to allow redelivery
                    consumer.nack();
                }
            }).build();

            // Add listener for subscriber lifecycle events, especially failure
            subscriber.addListener(new Subscriber.Listener() {
                @Override
                public void failed(State from, Throwable failure) {
                    System.err.println(getLocalName() + " - Subscriber failed: " + failure.getMessage());
                    failure.printStackTrace();
                }
            }, MoreExecutors.directExecutor());

            // Start subscriber asynchronously and wait until it is running
            subscriber.startAsync().awaitRunning();
            System.out.println(getLocalName() + " is now listening for messages...");

        } catch (Exception e) {
            System.err.println(getLocalName() + " - Exception in startSubscriber: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Cleanup method called when the agent is terminated.
     * Stops the Pub/Sub subscriber cleanly.
     */
    @Override
    protected void takeDown() {
        try {
            if (subscriber != null) {
                subscriber.stopAsync();
                System.out.println(getLocalName() + ": Subscriber stopped.");
            }
        } catch (Exception e) {
            System.err.println(getLocalName() + " - Error during shutdown: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
