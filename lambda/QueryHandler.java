import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Map;

public class QueryHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    // AWS clients
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonS3 s3Client;
    private final AmazonSNS snsClient;

    // Configuration parameters
    private final String dynamoDBTableName;
    private final String sqsUrl;

    // Constructor to initialize the AWS clients and read configuration from environment variables
    public QueryHandler() {
        // Read the AWS region from environment variables
        String region = System.getenv("AWS_REGION");

        // Initialize AWS clients with the specified region
        this.dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
        this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
        this.snsClient = AmazonSNSClientBuilder.standard().withRegion(region).build();

        // Read DynamoDB table name and SQS URL from environment variables
        this.dynamoDBTableName = System.getenv("DYNAMODB_TABLE_NAME");
        this.sqsUrl = System.getenv("SQS_URL");
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        // Extract the HTTP path from the ALB event
        Map<String, String> requestContext = (Map<String, String>) event.get("requestContext");
        String path = requestContext.get("path");

        // Delegate to the appropriate handler based on the path
        switch (path) {
            case "/executeQuery":
                return ExecuteQueryHandler.handle(event, dynamoDBClient, s3Client, dynamoDBTableName, sqsUrl);
            case "/reportStatus":
                return ReportStatusHandler.handle(event, dynamoDBClient, snsClient, dynamoDBTableName);
            default:
                return generateResponse(404, "Invalid Path");
        }
    }

    // Helper method to generate HTTP responses
    private Map<String, Object> generateResponse(int statusCode, String body) {
        return Map.of(
            "statusCode", statusCode,
            "headers", Map.of("Content-Type", "application/json"),
            "body", String.format("{\"message\": \"%s\"}", body)
        );
    }
}
