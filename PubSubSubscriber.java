package org.example.Agents;
import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubSubscriber {

    private final String projectId;
    private final String subscriptionId;
    private final String agentName;

    public PubSubSubscriber(String projectId, String subscriptionId, String agentName) {
        this.projectId = projectId;
        this.subscriptionId = subscriptionId;
        this.agentName = agentName;
    }

    public void start() {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(subscriptionName, (PubsubMessage message, AckReplyConsumer consumer) -> {
            System.out.println(agentName + " received message: " + message.getData().toStringUtf8());

            // Process message here...
            consumer.ack();

        });

        Subscriber subscriber = subscriberBuilder.build();

        subscriber.addListener(new Subscriber.Listener() {
            @Override
            public void failed(State from, Throwable failure) {
                System.err.println("Subscriber failed: " + failure);
            }
        }, MoreExecutors.directExecutor());

        subscriber.startAsync().awaitRunning();
        System.out.println(agentName + ": Subscriber started...");
    }


}
