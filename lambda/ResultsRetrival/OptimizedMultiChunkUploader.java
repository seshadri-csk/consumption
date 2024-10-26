import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class OptimizedMultiChunkUploader {

    private static final String BUCKET_NAME = "your-s3-bucket-name";
    private static final String KEY_NAME = "output.json"; // Or output.csv based on format
    private static final int CHUNK_SIZE_THRESHOLD = 5 * 1024 * 1024; // 5 MB threshold for multi-part

    private final MultiChunkDataConverter.OutputFormat outputFormat;
    private final S3AsyncClient s3Client;

    public OptimizedMultiChunkUploader(MultiChunkDataConverter.OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
        this.s3Client = S3AsyncClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    public void uploadChunks(String initialResponseJson) throws Exception {
        // Initiate the multipart upload
        String uploadId = initiateMultipartUpload();

        // Fetch and upload all chunks
        String nextChunkLink = fetchAndProcessChunk(initialResponseJson, uploadId, 1);

        int partNumber = 2;
        while (nextChunkLink != null) {
            nextChunkLink = fetchAndProcessChunk(fetchChunkData(nextChunkLink), uploadId, partNumber);
            partNumber++;
        }

        // Complete the multipart upload
        completeMultipartUpload(uploadId);
    }

    // Fetches and processes a chunk, uploads the data, and returns the next chunk link
    private String fetchAndProcessChunk(String chunkDataJson, String uploadId, int partNumber) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode chunkResponse = mapper.readTree(chunkDataJson);

        // Extract schema and data array from the chunk response
        List<Map<String, Object>> schema = extractSchema(chunkResponse);
        List<List<Object>> dataArray = extractDataArray(chunkResponse);

        // Convert chunk data to the specified format
        String chunkData = MultiChunkDataConverter.convertToFormat(schema, dataArray, outputFormat);

        // Upload the chunk to S3
        uploadPartToS3(chunkData, uploadId, partNumber);

        // Return the next chunk link (if any)
        JsonNode nextChunkLinkNode = chunkResponse.path("next_chunk_internal_link");
        return nextChunkLinkNode.isMissingNode() ? null : nextChunkLinkNode.asText();
    }

    // Fetches chunk data from the next chunk link
    private String fetchChunkData(String chunkLink) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(chunkLink).openConnection();
        connection.setRequestMethod("GET");

        try (InputStream inputStream = connection.getInputStream()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    // Initiates multipart upload and returns the upload ID
    private String initiateMultipartUpload() {
        CreateMultipartUploadRequest createMultipartRequest = CreateMultipartUploadRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY_NAME)
                .build();

        return s3Client.createMultipartUpload(createMultipartRequest)
                .join()
                .uploadId();
    }

    // Uploads a chunk to S3 as part of the multi-part upload
    private void uploadPartToS3(String chunkData, String uploadId, int partNumber) {
        byte[] chunkBytes = chunkData.getBytes(StandardCharsets.UTF_8);

        UploadPartRequest uploadRequest = UploadPartRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY_NAME)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        CompletableFuture<Void> uploadFuture = s3Client.uploadPart(uploadRequest, AsyncRequestBody.fromBytes(chunkBytes));
        uploadFuture.join();  // Wait for completion
    }

    // Completes the multipart upload
    private void completeMultipartUpload(String uploadId) {
        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(BUCKET_NAME)
                .key(KEY_NAME)
                .uploadId(uploadId)
                .build();

        s3Client.completeMultipartUpload(completeRequest).join();
    }

    // Extracts the schema from the chunk response
    private List<Map<String, Object>> extractSchema(JsonNode chunkResponse) {
        // Extract schema from the manifest
        JsonNode schemaNode = rootNode.path("manifest").path("schema");

       List<Map<String, Object>> columnsList = new ArrayList<>();

        // Get the columns array from the schemaNode
        JsonNode columnsArray = schemaNode.get("columns");

        // Iterate through the columns array
        for (JsonNode columnNode : columnsArray) {
            Map<String, Object> columnMap = new HashMap<>();

            // Extract column properties and put them into the map
            columnMap.put("name", columnNode.get("name").asText());
            columnMap.put("position", columnNode.get("position").asInt());
            columnMap.put("type_name", columnNode.get("type_name").asText());
            columnMap.put("type_text", columnNode.get("type_text").asText());

            // Optional fields like type_precision and type_scale
            if (columnNode.has("type_precision")) {
                columnMap.put("type_precision", columnNode.get("type_precision").asInt());
            }
            if (columnNode.has("type_scale")) {
                columnMap.put("type_scale", columnNode.get("type_scale").asInt());
            }

            columnsList.add(columnMap);
        }

        return columnsList;

        return null;
    }

    // Extracts the data array from the chunk response
    private List<List<Object>> extractDataArray(JsonNode chunkResponse) {
        // Extract data array from the result node
        // Implement data array extraction logic from JSON
         // Get data_array node
        JsonNode dataArrayNode = rootNode.path("result").path("data_array");
       
        List<List<Object>> dataArray = new ArrayList<>();

        // Iterate through rows in the data_array
        for (JsonNode rowNode : dataArrayNode) {
            List<Object> row = new ArrayList<>();
            // Add each value in the row to the list
            for (JsonNode valueNode : rowNode) {
                row.add(valueNode.asText());  // Convert to text, you can modify based on the type
            }
            dataArray.add(row);
        }
        
        return dataArray;
    }
}
