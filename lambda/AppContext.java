import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import com.amazonaws.services.lambda.runtime.Context;

public class AppContext {
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final String tableName;
    private final String reportStatusQueueUrl;
    private final Context lambdaContext;  // Lambda context for logging

    // Constructor to initialize shared resources and lambda context
    public AppContext(DynamoDbClient dynamoDbClient, SqsClient sqsClient, String tableName, String reportStatusQueueUrl, Context lambdaContext) {
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
        this.tableName = tableName;
        this.reportStatusQueueUrl = reportStatusQueueUrl;
        this.lambdaContext = lambdaContext;
    }

    // Getters for the shared resources
    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }

    public SqsClient getSqsClient() {
        return sqsClient;
    }

    public String getTableName() {
        return tableName;
    }

    public String getReportStatusQueueUrl() {
        return reportStatusQueueUrl;
    }

    // Get the Lambda context for logging
    public Context getLambdaContext() {
        return lambdaContext;
    }
}
