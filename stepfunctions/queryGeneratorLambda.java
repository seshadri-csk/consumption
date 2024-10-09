import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class QueryGeneratorLambda implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
    private final String REPORTS_CONFIG_TABLE = "ReportConfigTable";  // Replace with your table name
    private final String STATUS_TABLE = "QueryStatusTable";  // Replace with your status table name

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {

        String reportName = (String) event.get("report_name");
        Map<String, Object> parameters = (Map<String, Object>) event.get("parameters");

        // Generate unique request ID
        String requestId = UUID.randomUUID().toString();

        try {
            // Fetch the report configuration from DynamoDB
            Item reportConfig = fetchReportConfig(reportName);

            // Check if the SQL override is true
            boolean sqlOverride = reportConfig.getBoolean("sql-override");
            String sqlQuery;

            if (sqlOverride) {
                // Use the pre-defined SQL query from the report configuration
                sqlQuery = reportConfig.getString("default_sql");
                context.getLogger().log("Using SQL from report configuration: " + sqlQuery);
            } else {
                // Dynamically generate the SQL query using parameters
                sqlQuery = generateSQLQuery(reportConfig, parameters);
                context.getLogger().log("Dynamically generated SQL query: " + sqlQuery);
            }

            // Store the generated SQL and current workflow state in DynamoDB
            saveWorkflowState(requestId, reportName, sqlQuery, "QueryGenerated");

            // Prepare response
            Map<String, Object> response = new HashMap<>();
            response.put("request_id", requestId);
            response.put("sql_query", sqlQuery);
            return response;

        } catch (Exception e) {
            context.getLogger().log("Error generating query: " + e.getMessage());
            throw new RuntimeException("Failed to generate query.");
        }
    }

    // Fetch the report configuration from DynamoDB
    private Item fetchReportConfig(String reportName) {
        Table table = dynamoDB.getTable(REPORTS_CONFIG_TABLE);
        return table.getItem("report_name", reportName);  // Assumes report_name is the partition key
    }

    // Dynamically generate the SQL query
    private String generateSQLQuery(Item reportConfig, Map<String, Object> parameters) {
        String baseQuery = reportConfig.getString("default_sql");
        // Replace placeholders in base SQL query with actual parameter values
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            baseQuery = baseQuery.replace("{" + entry.getKey() + "}", entry.getValue().toString());
        }
        return baseQuery;
    }

    // Store the generated SQL and current state in DynamoDB
    private void saveWorkflowState(String requestId, String reportName, String sqlQuery, String workflowState) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("request_id", AttributeValue.builder().s(requestId).build());
        item.put("report_name", AttributeValue.builder().s(reportName).build());
        item.put("sql_query", AttributeValue.builder().s(sqlQuery).build());
        item.put("workflow_state", AttributeValue.builder().s(workflowState).build());

        // Save the item in the status DynamoDB table
        DynamoDbClient client = DynamoDbClient.builder().region(Region.US_EAST_1).build();
        PutItemRequest request = PutItemRequest.builder().tableName(STATUS_TABLE).item(item).build();
        client.putItem(request);
    }
}
