package org.example.Agents;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jade.core.Agent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * GCSUploaderAgent
 * -----------------
 * A JADE agent that:
 *  1. Converts timestamps in a JSONL file from ms to s
 *  2. Uploads the file to Google Cloud Storage (GCS)
 *
 * Arguments expected: [bucketName, objectName, localFilePath, credentialsFilePath]
 */
public class GCSUploaderAgent extends Agent {

    private Storage storage;

    @Override
    protected void setup() {
        System.out.println(getLocalName() + "Starting GCS upload process...");

        // Read arguments
        Object[] args = getArguments();
        if (args == null || args.length < 4) {
            System.err.println(" Missing arguments. Usage: bucketName, objectName, localFilePath, credentialsFilePath");
            doDelete();
            return;
        }

        String bucketName = args[0].toString().trim();
        String objectName = args[1].toString().trim();
        String originalFilePath = args[2].toString().trim();
        String credentialsPath = args[3].toString().trim();

        // Initialize GCS client with explicit credentials
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            storage = StorageOptions.newBuilder()
                    .setCredentials(ServiceAccountCredentials.fromStream(serviceAccountStream))
                    .build()
                    .getService();

            if (storage == null) {
                throw new IllegalStateException("GCS Storage client initialization failed. `storage` is null.");
            }
        } catch (Exception e) {
            System.err.println(" Failed to initialize GCS client with explicit credentials.");
            e.printStackTrace();
            doDelete();
            return;
        }

        try {
            if (originalFilePath.endsWith(".jsonl")) {
                // Convert timestamps in JSONL and upload
                String fixedFileName = "fixed_" + Paths.get(originalFilePath).getFileName();
                convertJsonlTimestamps(originalFilePath, fixedFileName);
                uploadFileToGCS(bucketName, objectName, fixedFileName);

            } else if (originalFilePath.endsWith(".csv")) {
                // Direct upload for CSV
                uploadFileToGCS(bucketName, objectName, originalFilePath);

            } else {
                // Unsupported file type
                System.err.println(" Unsupported file type for upload: " + originalFilePath);
            }

            System.out.println(" Upload completed: gs://" + bucketName + "/" + objectName);

        } catch (Exception e) {
            System.err.println(" Error during file processing or upload: " + e.getMessage());
            e.printStackTrace();
        } finally {
            doDelete(); // Always clean up
        }
    }

    /**
     * Converts timestamps in JSONL file from milliseconds to seconds.
     */
    private void convertJsonlTimestamps(String inputPathStr, String outputPathStr) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path inputPath = Paths.get(inputPathStr);
        Path outputPath = Paths.get(outputPathStr);

        try (
                BufferedReader reader = Files.newBufferedReader(inputPath);
                BufferedWriter writer = Files.newBufferedWriter(outputPath)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().startsWith("{")) continue;

                ObjectNode json = (ObjectNode) mapper.readTree(line);
                if (json.has("timestamp")) {
                    long ms = json.get("timestamp").asLong();
                    json.put("timestamp", ms / 1000); // convert to seconds
                }

                writer.write(mapper.writeValueAsString(json));
                writer.newLine();
            }
        }
    }

    /**
     * Uploads file to Google Cloud Storage.
     */
    private void uploadFileToGCS(String bucketName, String objectName, String filePath) throws Exception {
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        byte[] content = Files.readAllBytes(path);
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // Upload to GCS
        storage.create(blobInfo, content);
    }
}
