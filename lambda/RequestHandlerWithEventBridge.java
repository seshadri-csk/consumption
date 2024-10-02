import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.eventbridge.AmazonEventBridge;
import com.amazonaws.services.eventbridge.AmazonEventBridgeClientBuilder;
import com.amazonaws.services.eventbridge.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WPDPQueryRequestHandler implements RequestHandler<Map<String, Object>, Map<String, String>> {

    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final AmazonEventBridge eventBridgeClient = AmazonEventBridgeClientBuilder.defaultClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Define the SQS queue and table names
    private static final String SQS_QUEUE_URL = "https://sqs.region.amazonaws.com/account-id/wp-dp-q-request-queue";
    private static final String DYNAMO_TABLE_NAME = "YourDynamoDBTable";

    @Override
    public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
        Map<String, String> response = new HashMap<>();

        try {
            // Validate the input (assume there's a valid field 'scheduleExpression' or we provide a default)
            if (!input.containsKey("parameters")) {
                response.put("statusCode", "400");
                response.put("message", "Invalid request body. Missing parameters.");
                return response;
            }

            // Generate a unique request ID
            String requestId = UUID.randomUUID().toString();

            // Store parameters in DynamoDB with the request ID
            storeParametersInDynamoDB(requestId, input, context);

            // Check if there's a scheduled expression for this request
            if (input.containsKey("scheduleExpression")) {
                String scheduleExpression = (String) input.get("scheduleExpression");
                createEventBridgeRule(requestId, scheduleExpression, context);
            } else {
                // No scheduling condition, so publish the message to SQS
                publishMessageToSQS(requestId, input, context);
            }

            // Return the request ID to the client
            response.put("statusCode", "200");
            response.put("requestId", requestId);
        } catch (Exception e) {
            context.getLogger().log("Error processing request: " + e.getMessage());
            response.put("statusCode", "500");
            response.put("message", "Internal server error.");
        }

        return response;
    }

    private void storeParametersInDynamoDB(String requestId, Map<String, Object> input, Context context) {
        // Store the request parameters in DynamoDB with the requestId
        Map<String, Object> item = new HashMap<>();
        item.put("RequestId", requestId);
        item.put("Parameters", input);

        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(DYNAMO_TABLE_NAME)
                .withItem((Map<String, Object>) objectMapper.convertValue(item, Map.class));

        dynamoDBClient.putItem(putItemRequest);
        context.getLogger().log("Stored parameters in DynamoDB for request ID: " + requestId);
    }

    private void publishMessageToSQS(String requestId, Map<String, Object> input, Context context) {
        // Publish the request to SQS
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(SQS_QUEUE_URL)
                .withMessageBody(objectMapper.writeValueAsString(input))
                .addMessageAttributesEntry("RequestId", new MessageAttributeValue().withDataType("String").withStringValue(requestId));

        sqsClient.sendMessage(sendMessageRequest);
        context.getLogger().log("Published message to SQS for request ID: " + requestId);
    }

    private void createEventBridgeRule(String requestId, String scheduleExpression, Context context) {
        // Create a new EventBridge rule with the given schedule expression
        PutRuleRequest putRuleRequest = new PutRuleRequest()
                .withName("wp-dp-rule-" + requestId)
                .withScheduleExpression(scheduleExpression)
                .withState(RuleState.ENABLED)
                .withDescription("Scheduled Event for request ID: " + requestId);

        PutRuleResult ruleResult = eventBridgeClient.putRule(putRuleRequest);
        String ruleArn = ruleResult.getRuleArn();

        context.getLogger().log("Created EventBridge rule with ARN: " + ruleArn);

        // Add the Lambda target (wp-dp-api-handler) to the rule
        Target target = new Target()
                .withId("wp-dp-api-handler")
                .withArn("arn:aws:lambda:region:account-id:function:wp-dp-api-handler")
                .withInput("{\"RequestId\":\"" + requestId + "\"}");

        PutTargetsRequest putTargetsRequest = new PutTargetsRequest()
                .withRule(putRuleRequest.getName())
                .withTargets(target);

        eventBridgeClient.putTargets(putTargetsRequest);
        context.getLogger().log("Added Lambda target to EventBridge rule for request ID: " + requestId);
    }
}
