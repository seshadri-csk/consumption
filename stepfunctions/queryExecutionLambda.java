import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;

public class QueryExecution implements RequestHandler<Map<String, Object>, String> {
    private static final String TABLE_NAME = "RequestStatusTable";
    private static final String S3_BUCKET_NAME = "your-s3-bucket";
    private static final long MAX_EXECUTION_TIME = 60000L; // 60 seconds

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        String requestId = (String) event.get("request_id");
        long startTime = System.currentTimeMillis();
        DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClient.builder().build());
        Table table = dynamoDB.getTable(TABLE_NAME);
        
        // Step 1: Retrieve SQL from DynamoDB
        Item requestItem = table.getItem("RequestId", requestId);
        if (requestItem == null) {
            throw new RuntimeException("Request ID not found in DynamoDB");
        }
        String sqlQuery = requestItem.getString("sql_query");

        // Step 2: Invoke Databricks REST API to execute query
        String databricksExecutionId = invokeDatabricksAPI(sqlQuery);

        // Step 3: Store execution status and Databricks execution ID in DynamoDB
        table.updateItem("RequestId", requestId,
                "SET databricksExecutionId = :executionId, status = :status",
                new ValueMap().withString(":executionId", databricksExecutionId).withString(":status", "RUNNING"));

        // Step 4: Check query execution status in a loop
        while (System.currentTimeMillis() - startTime < MAX_EXECUTION_TIME) {
            String queryStatus = checkDatabricksQueryStatus(databricksExecutionId);

            if ("COMPLETED".equals(queryStatus)) {
                // Step 4a: Retrieve results and store them in S3
                String queryResults = retrieveDatabricksResults(databricksExecutionId);
                storeResultsInS3(S3_BUCKET_NAME, requestId, queryResults);

                // Step 4b: Update DynamoDB with status "COMPLETE" and exit loop
                table.updateItem("RequestId", requestId,
                        "SET status = :status",
                        new ValueMap().withString(":status", "COMPLETE"));
                return "Query completed and results stored in S3.";
            } else if ("ERROR".equals(queryStatus)) {
                throw new RuntimeException("Query execution failed.");
            }

            // Wait for 2 seconds before the next status check
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // If the query didn't complete in 60 seconds, update DynamoDB and exit
        table.updateItem("RequestId", requestId,
                "SET status = :status",
                new ValueMap().withString(":status", "TIMEOUT"));
        return "Query timed out after 60 seconds.";
    }

    private String invokeDatabricksAPI(String sqlQuery) {
        String databricksExecutionId = null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost postRequest = new HttpPost("https://databricks-api-url/api/2.0/sql/statements");
            postRequest.setHeader("Authorization", "Bearer YOUR_DATABRICKS_TOKEN");
            postRequest.setEntity(new StringEntity("{ \"query\": \"" + sqlQuery + "\" }", ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                String responseBody = EntityUtils.toString(response.getEntity());
                // Extract execution ID from the Databricks API response (pseudo-code)
                databricksExecutionId = extractExecutionId(responseBody);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to invoke Databricks API", e);
        }
        return databricksExecutionId;
    }

    private String checkDatabricksQueryStatus(String databricksExecutionId) {
        // Logic to call Databricks API to check query execution status (pseudo-code)
        return "COMPLETED";  // or "RUNNING", or "ERROR"
    }

    private String retrieveDatabricksResults(String databricksExecutionId) {
        // Logic to retrieve results from Databricks (pseudo-code)
        return "query results";
    }

    private void storeResultsInS3(String bucketName, String requestId, String results) {
        // Logic to store query results in S3 using the AWS SDK
    }

    private String extractExecutionId(String responseBody) {
        // Logic to extract Databricks execution ID from API response (pseudo-code)
        return "execution-id";
    }
}
