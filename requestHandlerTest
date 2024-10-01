import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.lambda.runtime.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.HashMap;
import java.util.Map;

public class WPDPQueryRequestHandlerTest {

    // Mock the AWS SDK clients
    @Mock
    private DynamoDbClient mockDynamoDbClient;

    @Mock
    private SqsClient mockSqsClient;

    @Mock
    private Context mockContext;

    // Inject mocks into the class being tested
    @InjectMocks
    private WPDPQueryRequestHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Set up environment variables for the test
        System.setProperty("DYNAMODB_TABLE_NAME", "wp-dp-wf-request-state");
        System.setProperty("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/wp-df-request-queue");
    }

    @Test
    public void testHandleRequest_ValidInput() throws Exception {
        // Mock the input event
        Map<String, Object> input = new HashMap<>();
        input.put("tableName", "sales");
        input.put("columns", new String[]{"product_name", "total_sales", "sales_date"});
        input.put("filters", Map.of("dateRange", Map.of("column", "sales_date", "startDate", "2024-01-01", "endDate", "2024-12-31")));

        // Mock the DynamoDB response
        PutItemResponse putItemResponse = mock(PutItemResponse.class);
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(putItemResponse);

        // Mock the SQS response
        SendMessageResponse sendMessageResponse = SendMessageResponse.builder().messageId("12345").build();
        when(mockSqsClient.sendMessage(any(SendMessageRequest.class))).thenReturn(sendMessageResponse);

        // Invoke the handler
        Map<String, Object> response = handler.handleRequest(input, mockContext);

        // Validate the response
        assertEquals(200, response.get("statusCode"));
        assertNotNull(response.get("requestId"));

        // Verify that DynamoDB and SQS were called
        verify(mockDynamoDbClient).putItem(any(PutItemRequest.class));
        verify(mockSqsClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequest_MissingRequiredFields() {
        // Mock the input with missing required fields
        Map<String, Object> input = new HashMap<>();
        input.put("tableName", "sales");
        // Missing "columns" and "filters"

        // Invoke the handler
        Map<String, Object> response = handler.handleRequest(input, mockContext);

        // Validate the response
        assertEquals(400, response.get("statusCode"));
        assertEquals("Invalid request: missing required fields", response.get("message"));

        // Verify that DynamoDB and SQS were not called
        verify(mockDynamoDbClient, never()).putItem(any(PutItemRequest.class));
        verify(mockSqsClient, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequest_DynamoDbError() {
        // Mock the input event
        Map<String, Object> input = new HashMap<>();
        input.put("tableName", "sales");
        input.put("columns", new String[]{"product_name", "total_sales", "sales_date"});
        input.put("filters", Map.of("dateRange", Map.of("column", "sales_date", "startDate", "2024-01-01", "endDate", "2024-12-31")));

        // Mock a DynamoDB error
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenThrow(new RuntimeException("DynamoDB error"));

        // Invoke the handler
        Map<String, Object> response = handler.handleRequest(input, mockContext);

        // Validate the response
        assertEquals(500, response.get("statusCode"));
        assertEquals("Internal server error", response.get("message"));

        // Verify that DynamoDB was called but SQS was not
        verify(mockDynamoDbClient).putItem(any(PutItemRequest.class));
        verify(mockSqsClient, never()).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    public void testHandleRequest_SqsError() throws Exception {
        // Mock the input event
        Map<String, Object> input = new HashMap<>();
        input.put("tableName", "sales");
        input.put("columns", new String[]{"product_name", "total_sales", "sales_date"});
        input.put("filters", Map.of("dateRange", Map.of("column", "sales_date", "startDate", "2024-01-01", "endDate", "2024-12-31")));

        // Mock the DynamoDB response
        PutItemResponse putItemResponse = mock(PutItemResponse.class);
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(putItemResponse);

        // Mock an SQS error
        when(mockSqsClient.sendMessage(any(SendMessageRequest.class))).thenThrow(new RuntimeException("SQS error"));

        // Invoke the handler
        Map<String, Object> response = handler.handleRequest(input, mockContext);

        // Validate the response
        assertEquals(500, response.get("statusCode"));
        assertEquals("Internal server error", response.get("message"));

        // Verify that both DynamoDB and SQS were called
        verify(mockDynamoDbClient).putItem(any(PutItemRequest.class));
        verify(mockSqsClient).sendMessage(any(SendMessageRequest.class));
    }
}
