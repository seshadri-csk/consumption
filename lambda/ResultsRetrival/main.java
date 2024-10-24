public class Main {
    public static void main(String[] args) throws Exception {
        // Example initial response JSON from the first chunk
        String initialResponseJson = "{...}";  // Replace with actual initial response

        // Create a converter and uploader for JSON output
        MultiChunkDataConverter.OutputFormat outputFormat = MultiChunkDataConverter.OutputFormat.JSON; // or CSV
        OptimizedMultiChunkUploader uploader = new OptimizedMultiChunkUploader(outputFormat);

        // Upload all chunks to S3
        uploader.uploadChunks(initialResponseJson);
    }
}
