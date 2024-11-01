import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class WPDPApiHandlerTest {

    @Mock
    private AmazonSQS sqsClient;

    @Mock
    private AWSStepFunctions stepFunctionsClient;

    @Mock
    private Context context;

    @InjectMocks
    private WPDPApiHandler handler;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new WPDPApiHandler();
    }

    @Test
    void handleRequest_shouldProcessSqsEventAndTriggerStepFunction() throws Exception {
        // Arrange
        SQSEvent sqsEvent = new SQSEvent();
        SQSEvent.SQSMessage sqsMessage = new SQSEvent.SQSMessage();
        sqsMessage.setBody(objectMapper.writeValueAsString(Map.of("key", "value")));
        sqsMessage.setReceiptHandle("receipt-handle");
        sqsEvent.setRecords(List.of(sqsMessage));

        StartExecutionResult startExecutionResult = new StartExecutionResult().withExecutionArn("arn:aws:states:executionArn");
        when(stepFunctionsClient.startExecution(any(StartExecutionRequest.class))).thenReturn(startExecutionResult);

        // Act
        String result = handler.handleRequest(sqsEvent, context);

        // Assert
        assertEquals("Success", result);
        verify(stepFunctionsClient, times(1)).startExecution(any(StartExecutionRequest.class));
        verify(sqsClient, times(1)).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void handleRequest_shouldProcessScheduledEvent() {
        // Arrange
        ScheduledEvent scheduledEvent = new ScheduledEvent();
        Map<String, Object> detail = new HashMap<>();
        detail.put("eventKey", "eventValue");
        scheduledEvent.setDetail(detail);

        StartExecutionResult startExecutionResult = new StartExecutionResult().withExecutionArn("arn:aws:states:executionArn");
        when(stepFunctionsClient.startExecution(any(StartExecutionRequest.class))).thenReturn(startExecutionResult);

        // Act
        String result = handler.handleRequest(scheduledEvent, context);

        // Assert
        assertEquals("Success", result);
        verify(stepFunctionsClient, times(1)).startExecution(any(StartExecutionRequest.class));
        verifyNoInteractions(sqsClient);  // No SQS message to delete
    }

    @Test
    void handleRequest_shouldReturnErrorForUnsupportedEventType() {
        // Arrange
        Object unknownEvent = new Object();

        // Act
        String result = handler.handleRequest(unknownEvent, context);

        // Assert
        assertEquals("Error: Unsupported event type.", result);
        verifyNoInteractions(stepFunctionsClient, sqsClient);
    }

    @Test
    void triggerStepFunction_shouldReturnFalse_whenStepFunctionFails() {
        // Arrange
        Map<String, Object> payload = Map.of("key", "value");
        when(stepFunctionsClient.startExecution(any(StartExecutionRequest.class))).thenThrow(new RuntimeException("Step Function error"));

        // Act
        boolean result = handler.triggerStepFunction(payload, context);

        // Assert
        assertEquals(false, result);
        verify(context.getLogger()).log(contains("Error triggering Step Function"));
    }

    @Test
    void deleteMessageFromSQS_shouldLogError_whenDeleteFails() {
        // Arrange
        String receiptHandle = "invalid-receipt-handle";
        String queueUrl = "test-queue-url";
        doThrow(new RuntimeException("Delete error")).when(sqsClient).deleteMessage(any(DeleteMessageRequest.class));

        // Act
        handler.deleteMessageFromSQS(receiptHandle, queueUrl, context);

        // Assert
        verify(context.getLogger()).log(contains("Failed to delete message from SQS"));
    }
}
