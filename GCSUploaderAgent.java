package org.example.Agents;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jade.core.Agent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * GCSUploaderAgent
 * -----------------
 * A JADE agent that uploads a specified local file to a Google Cloud Storage (GCS) bucket.
 *
 * Arguments expected: [bucketName, objectName, localFilePath]
 *
 * Example:
 *   java -cp yourjar.jar jade.Boot -agents "uploader:org.example.Agents.GCSUploaderAgent(bucket-name, object-name.csv, /path/to/file.csv)"
 */
public class GCSUploaderAgent extends Agent {

    // GCS client
    private Storage storage;

    @Override
    protected void setup() {
        System.out.println( getLocalName() + " Uploads files and reports to Google Cloud Storage....");

        // ✅ Initialize GCS client with Application Default Credentials
        try {
            storage = StorageOptions.getDefaultInstance().getService();
        } catch (Exception e) {
            System.err.println(" Failed to initialize GCS client. Ensure credentials are configured.");
            e.printStackTrace();
            doDelete();
            return;
        }

        // ✅ Validate agent arguments
        Object[] args = getArguments();
        if (args == null || args.length < 3) {
            System.err.println(" Missing arguments. Usage: bucketName, objectName, localFilePath");
            doDelete();
            return;
        }

        String bucketName = args[0].toString().trim();
        String objectName = args[1].toString().trim();
        String filePath = args[2].toString().trim();

        // ✅ Upload file
        try {
            uploadFileToGCS(bucketName, objectName, filePath);
            System.out.println(" Upload complete. File: " + filePath + " -> gs://" + bucketName + "/" + objectName);
        } catch (Exception e) {
            System.err.println(" Upload failed: " + e.getMessage());
            e.printStackTrace();
        }

        // 🔚 Stop agent after operation
        doDelete();
    }

    /**
     * Uploads a local file to a GCS bucket.
     *
     * @param bucketName GCS bucket name (must exist)
     * @param objectName Desired name/path for object in bucket
     * @param filePath   Full local path to the file
     * @throws Exception if the file cannot be read or uploaded
     */
    private void uploadFileToGCS(String bucketName, String objectName, String filePath) throws Exception {
        Path path = Paths.get(filePath);

        // ⚠️ Validate file existence
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        // ✅ Read file content
        byte[] content = Files.readAllBytes(path);

        // ✅ Define the object ID and metadata
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // ✅ Upload the object
        storage.create(blobInfo, content);
    }
}
