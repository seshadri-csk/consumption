import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.AmazonEventBridgeClientBuilder;
import com.amazonaws.services.eventbridge.model.PutEventsRequest;
import com.amazonaws.services.eventbridge.model.PutEventsRequestEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WPDPQueryRequestHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final AmazonEventBridge eventBridgeClient = AmazonEventBridgeClientBuilder.defaultClient();
    private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String DYNAMO_TABLE_NAME = "YourDynamoDBTable";
    private static final String SQS_QUEUE_URL = "https://sqs.region.amazonaws.com/your-account-id/wp-dp-q-request-queue";
    private static final String EVENTBRIDGE_RULE_ARN = "arn:aws:events:region:account-id:rule/your-event-bridge-rule";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        
        try {
            // 1. Validate the request
            Map<String, Object> requestBody = validateRequest(input.getBody());
            if (requestBody == null) {
                return response.withStatusCode(400).withBody("Invalid request body");
            }

            // 2. Generate Request ID and store data in DynamoDB
            String requestId = UUID.randomUUID().toString();
            storeRequestInDynamoDB(requestId, requestBody);

            // 3. Handle SQS or EventBridge rule based on scheduling condition
            if (requestBody.containsKey("scheduleExpression")) {
                triggerEventBridgeRule(requestId, requestBody);
            } else {
                sendMessageToSQS(requestId, requestBody);
            }

            // 4. Return Request ID to client
            Map<String, String> responseBody = new HashMap<>();
            responseBody.put("requestId", requestId);
            return response.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
        } catch (Exception e) {
            return response.withStatusCode(500).withBody("Internal server error: " + e.getMessage());
        }
    }

    private Map<String, Object> validateRequest(String body) throws Exception {
        if (body == null || body.isEmpty()) {
            return null;
        }

        Map<String, Object> requestBody = objectMapper.readValue(body, Map.class);
        // Add your validation logic here (e.g., checking required fields)
        if (!requestBody.containsKey("reportType")) {
            return null;
        }
        return requestBody;
    }

    private void storeRequestInDynamoDB(String requestId, Map<String, Object> requestBody) {
        Table table = dynamoDB.getTable(DYNAMO_TABLE_NAME);
        Map<String, Object> item = new HashMap<>(requestBody);
        item.put("requestId", requestId);
        PutItemRequest request = new PutItemRequest()
                .withTableName(DYNAMO_TABLE_NAME)
                .withItem(item);
        dynamoDBClient.putItem(request);
    }

    private void sendMessageToSQS(String requestId, Map<String, Object> requestBody) {
        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(SQS_QUEUE_URL)
                .withMessageBody("RequestId: " + requestId + ", Parameters: " + requestBody.toString());
        sqsClient.sendMessage(sendMsgRequest);
    }

    private void triggerEventBridgeRule(String requestId, Map<String, Object> requestBody) {
        PutEventsRequestEntry requestEntry = new PutEventsRequestEntry()
                .withSource("custom.wp-dp-query")
                .withDetailType("Scheduled Report")
                .withDetail("{\"requestId\":\"" + requestId + "\",\"parameters\":" + requestBody.toString() + "}")
                .withResources(EVENTBRIDGE_RULE_ARN);
        
        PutEventsRequest eventsRequest = new PutEventsRequest().withEntries(requestEntry);
        eventBridgeClient.putEvents(eventsRequest);
    }
}
