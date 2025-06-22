import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import jade.wrapper.StaleProxyException;
import jade.core.Profile;
import jade.core.ProfileImpl;
import jade.wrapper.AgentContainer;
import jade.wrapper.AgentController;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        String projectId = "multi-agent-hackathon";
        Publisher rawDataPublisher = null;
        Publisher bottleneckPublisher = null;

        try {
            // ✅ Initialize Google Cloud Pub/Sub publishers
            rawDataPublisher = Publisher.newBuilder(TopicName.of(projectId, "machine-metrics-topic")).build();
            bottleneckPublisher = Publisher.newBuilder(TopicName.of(projectId, "bottleneck-topic")).build();
            System.out.println("Google Cloud Pub/Sub publishers initialized.");
        } catch (IOException e) {
            System.err.println(" Failed to initialize Pub/Sub publishers:");
            e.printStackTrace();
            return;
        }

        // ✅ Initialize JADE runtime and main agent container
        jade.core.Runtime jadeRuntime = jade.core.Runtime.instance();
        Profile profile = new ProfileImpl(); // Uses default settings (localhost, port 1099)
        AgentContainer mainContainer = jadeRuntime.createMainContainer(profile);
        System.out.println(" JADE runtime initialized.");

        // ✅ Start all necessary JADE agents
        startAgent(mainContainer, "DataCollectorAgent", "org.example.Agents.DataCollectorAgentADK", null);
        startAgent(mainContainer, "ProcessorAgent", "org.example.Agents.ProcessorAgent", null);
        startAgent(mainContainer, "NotifierAgent", "org.example.Agents.NotifierAgent", null);
        startAgent(mainContainer, "PubSubAgent", "org.example.Agents.PubSubSubscriberAgent", null);
        startAgent(mainContainer, "BottleneckDetector", "org.example.Agents.BottleneckDetectorAgent", null);

        Object[] uploaderArgsJSON = new Object[]{
                "bottleneck-model-bucket",
                "machine_data.jsonl",
                "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.jsonl",
                "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\multi-agent-hackathon-5e880ee28778.json"
        };
        startAgent(mainContainer, "UploaderAgent_JSON", "org.example.Agents.GCSUploaderAgent", uploaderArgsJSON);

        Object[] uploaderArgsCSV = new Object[]{
                "bottleneck-model-bucket",
                "machine_data.csv",
                "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\src\\main\\resources\\org\\example\\data\\machine_data.csv",
                "C:\\Desktop\\Java\\BottleNeckTrackerAI-GCP\\BottleNeckTrackerAI\\multi-agent-hackathon-5e880ee28778.json"
        };
        startAgent(mainContainer, "UploaderAgent_CSV", "org.example.Agents.GCSUploaderAgent", uploaderArgsCSV);


        // BigQueryUtilAgent args: [projectId, datasetName, tableName]
        Object[] bigQueryArgs = new Object[]{projectId, "bottleneck_data", "machine_metrics"};
        startAgent(mainContainer, "BigQueryUtilAgent", "org.example.Agents.BigQueryUtil", bigQueryArgs);

        startAgent(mainContainer, "BigQueryLoaderAgent", "org.example.Agents.BigQueryLoaderAgent", null);
        startAgent(mainContainer, "EmailSchedulerAgent", "org.example.Agents.EmailSchedulerAgent", null);

        // ✅ Add JVM shutdown hook to cleanly shutdown Pub/Sub publishers
        Publisher finalRawDataPublisher = rawDataPublisher;
        Publisher finalBottleneckPublisher = bottleneckPublisher;

        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("JVM shutdown initiated. Cleaning up Pub/Sub publishers...");
            shutdownPublisher(finalRawDataPublisher);
            shutdownPublisher(finalBottleneckPublisher);
        }));
    }

    /**
     * Shuts down a Google Pub/Sub Publisher gracefully.
     */
    private static void shutdownPublisher(Publisher publisher) {
        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
                System.out.println(" Publisher shutdown completed.");
            } catch (Exception e) {
                System.err.println(" Error during publisher shutdown: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Starts a JADE agent with the provided name, class, and arguments.
     */
    private static void startAgent(AgentContainer container, String name, String className, Object[] args) {
        try {
            AgentController agent = container.createNewAgent(name, className, args);
            agent.start();
            System.out.println(" Agent started: " + name);
        } catch (StaleProxyException e) {
            System.err.println(" Failed to start agent: " + name);
            e.printStackTrace();
        }
    }
}
