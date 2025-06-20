import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import org.example.Agents.*;
import jade.core.Profile;
import jade.core.ProfileImpl;
import jade.wrapper.AgentContainer;
import jade.wrapper.AgentController;
import jade.core.Runtime;

public class App {
    public static void main(String[] args) throws Exception {
        String projectId = "multi-agent-hackathon";
        String subscriptionId = "bottleneck-sub";

        // Setup Pub/Sub publishers for the same topic (assuming bottleneck-topic)
        Publisher rawDataPublisher = Publisher.newBuilder(TopicName.of(projectId, "bottleneck-topic")).build();
        Publisher bottleneckPublisher = Publisher.newBuilder(TopicName.of(projectId, "bottleneck-topic")).build();

        // Resource path for machine_data.csv in the resources folder
        //String resourcePath = "src/main/resources/org/example/data/machine_data_50.csv";

        // Initialize and run DataCollectorAgent with the CSV resource path
//        DataCollectorAgent collector = new DataCollectorAgent("Collector", rawDataPublisher, resourcePath);
//        collector.performTask();

        // Initialize and run DataCollectorAgentADK (assumed another data collector)
        DataCollectorAgentADK dataCollector = new DataCollectorAgentADK(
                "DataCollectorAgent",
                projectId,
                "bottleneck-topic"
        );
        dataCollector.performTask();

        // Initialize and run BottleneckDetectorAgent
        BottleneckDetectorAgent detector = new BottleneckDetectorAgent("Detector", subscriptionId, bottleneckPublisher);
        detector.performTask();

        // Initialize and run InventoryAdvisorAgent
        InventoryAdvisorAgent advisor = new InventoryAdvisorAgent();
        advisor.start();
//        InventoryAdvisorAgent advisor = new InventoryAdvisorAgent(
//                "Advisor",
//                projectId,
//                subscriptionId,
//                "us-central1",
//                "2132081689118113792"
//        );
        //advisor.performTask();

        // Start a subscriber for listening to messages on the subscription

        String agentName = "BottleNeckTrackerAI Agent";

        if (projectId == null || subscriptionId == null) {
            System.err.println("Missing environment variables: GCP_PROJECT_ID or PUBSUB_SUBSCRIPTION_ID");
            return;
        }

        PubSubSubscriber subscriber = new PubSubSubscriber(projectId, subscriptionId, agentName);
        subscriber.start();

        // Keep main thread alive so subscriber listens for 60 seconds
        Thread.sleep(60000);

        //subscriber.stop();

        // Start JADE runtime and create container
        Runtime runtime = Runtime.instance();
        Profile profile = new ProfileImpl();
        AgentContainer container = runtime.createMainContainer(profile);

        AgentController processorAgent = container.createNewAgent("ProcessorAgent", ProcessorAgentADK.class.getName(), null);
        AgentController notifierAgent = container.createNewAgent("NotifierAgent", NotifierAgentADK.class.getName(), null);

        processorAgent.start();
        notifierAgent.start();
    }
}
