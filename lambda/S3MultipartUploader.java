import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.methods.HttpGet;
import org.apache.hc.core5.http.ParseException;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class S3MultipartUploader {

    private static final String BUCKET_NAME = System.getenv("BUCKET_NAME");
    private static final String S3_KEY = "large-upload.csv";
    private static final String API_BASE_URL = "https://api-provider.com/statement/";
    private static final long PART_SIZE = 5 * 1024 * 1024; // Minimum 5 MB per part

    public static void main(String[] args) {
        try {
            // Step 1: Get the initial external link (e.g., chunk 0)
            String firstExternalLink = "https://someplace.cloud-provider.com/very/long/path/chunk1";
            int totalChunks = 5; // Assume this comes from the initial API response
            
            // Step 2: Call method to upload all chunks to S3
            performMultiPartUpload(firstExternalLink, totalChunks);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void performMultiPartUpload(String firstExternalLink, int totalChunks) throws Exception {
        S3Client s3Client = S3Client.builder().build();
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // 1. Initiate Multi-part Upload
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(BUCKET_NAME)
                .key(S3_KEY)
                .build();
        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = createMultipartUploadResponse.uploadId();

        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;

        try {
            // 2. Upload First Chunk
            uploadChunkToS3(s3Client, httpClient, firstExternalLink, partNumber++, uploadId, completedParts);

            // 3. Upload Remaining Chunks (Sequentially or Concurrently based on use case)
            for (int i = 1; i < totalChunks; i++) {
                String chunkUrl = getExternalLinkForChunk(i);  // Get chunk's URL via API request
                uploadChunkToS3(s3Client, httpClient, chunkUrl, partNumber++, uploadId, completedParts);
            }

            // 4. Complete Multi-part Upload
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(S3_KEY)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build();
            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            System.out.println("Multi-part upload completed successfully!");

        } catch (Exception e) {
            // If error occurs, abort the multipart upload to avoid orphaned parts
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(S3_KEY)
                    .uploadId(uploadId)
                    .build();
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            throw new RuntimeException("Multipart upload aborted due to an error", e);

        } finally {
            httpClient.close();
        }
    }

    // Helper method to upload a chunk to S3
    public static void uploadChunkToS3(S3Client s3Client, CloseableHttpClient httpClient, String externalLink, int partNumber,
                                       String uploadId, List<CompletedPart> completedParts) throws Exception {
        HttpGet httpGet = new HttpGet(externalLink);

        try (CloseableHttpResponse response = httpClient.execute(httpGet);
             InputStream content = response.getEntity().getContent()) {

            // Create UploadPartRequest
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(S3_KEY)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .build();

            // Stream the content to S3 as a part
            UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest,
                    RequestBody.fromInputStream(content, response.getEntity().getContentLength()));

            // Record the ETag for each part uploaded
            completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadPartResponse.eTag())
                    .build());

            System.out.println("Uploaded part " + partNumber + " from " + externalLink);
        }
    }

    // Helper method to get the external link for the next chunk by calling the API
    public static String getExternalLinkForChunk(int chunkIndex) throws ParseException {
        // In a real-world scenario, you'd make an API call to retrieve the next external link
        String apiUrl = API_BASE_URL + chunkIndex;
        System.out.println("Fetching chunk: " + chunkIndex + " from API: " + apiUrl);

        // For the sake of the example, we simulate the URL generation
        return "https://someplace.cloud-provider.com/very/long/path/chunk" + chunkIndex;
    }
}
