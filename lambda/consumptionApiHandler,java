import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class WPDPApiHandler implements RequestHandler<Object, String> {

    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final AWSStepFunctions stepFunctionsClient = AWSStepFunctionsClientBuilder.defaultClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Step Function ARN
    private static final String STEP_FUNCTION_ARN = "arn:aws:states:region:account-id:stateMachine:YourStepFunction";

    @Override
    public String handleRequest(Object event, Context context) {
        try {
            // Handle SQS Events
            if (event instanceof SQSEvent) {
                SQSEvent sqsEvent = (SQSEvent) event;
                for (SQSEvent.SQSMessage sqsMessage : sqsEvent.getRecords()) {
                    // Extract and process SQS message
                    String messageBody = sqsMessage.getBody();
                    Map<String, Object> messagePayload = objectMapper.readValue(messageBody, Map.class);
                    context.getLogger().log("Processing SQS message: " + messageBody);

                    // Trigger the Step Function workflow
                    if (triggerStepFunction(messagePayload, context)) {
                        // Only delete message if Step Function started successfully
                        deleteMessageFromSQS(sqsMessage.getReceiptHandle(), sqsEvent.getRecords().get(0).getEventSourceArn(), context);
                    }
                }
            }
            // Handle Scheduled Events from EventBridge
            else if (event instanceof ScheduledEvent) {
                ScheduledEvent scheduledEvent = (ScheduledEvent) event;
                Map<String, Object> eventPayload = scheduledEvent.getDetail();
                context.getLogger().log("Processing Scheduled Event: " + eventPayload);
                triggerStepFunction(eventPayload, context);  // No need to delete message for EventBridge
            } else {
                context.getLogger().log("Unknown event type received.");
                return "Error: Unsupported event type.";
            }
            return "Success";
        } catch (Exception e) {
            context.getLogger().log("Error processing event: " + e.getMessage());
            return "Error: " + e.getMessage();
        }
    }

    private boolean triggerStepFunction(Map<String, Object> messagePayload, Context context) {
        try {
            StartExecutionRequest startExecutionRequest = new StartExecutionRequest()
                    .withStateMachineArn(STEP_FUNCTION_ARN)
                    .withInput(objectMapper.writeValueAsString(messagePayload));

            StartExecutionResult result = stepFunctionsClient.startExecution(startExecutionRequest);
            context.getLogger().log("Started Step Function execution with ARN: " + result.getExecutionArn());
            return true;  // Successfully started Step Function
        } catch (Exception e) {
            context.getLogger().log("Error triggering Step Function: " + e.getMessage());
            return false;  // Failed to start Step Function
        }
    }

    private void deleteMessageFromSQS(String receiptHandle, String queueUrl, Context context) {
        try {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withReceiptHandle(receiptHandle);
            sqsClient.deleteMessage(deleteMessageRequest);
            context.getLogger().log("Deleted message from SQS queue: " + queueUrl);
        } catch (Exception e) {
            context.getLogger().log("Failed to delete message from SQS: " + e.getMessage());
        }
    }
}
