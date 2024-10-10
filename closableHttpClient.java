import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.client5.http.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ParseException;
import java.io.IOException;

public class DatabricksQueryExecutor {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String DATABRICKS_URL = "https://<databricks-instance>/api/2.0/sql/queries";

    public static void main(String[] args) {
        try {
            // Example of invoking the Databricks API
            String statementId = executeDatabricksQuery();
            System.out.println("Extracted statement_id: " + statementId);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    public static String executeDatabricksQuery() throws IOException, ParseException {
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // Set up the POST request (this should be set up with the proper URL and headers)
        HttpPost postRequest = new HttpPost(DATABRICKS_URL);
        postRequest.setHeader("Authorization", "Bearer <your-token>");
        postRequest.setHeader("Content-Type", "application/json");

        // Execute the request and get the response
        try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
            // Check for successful status code
            int statusCode = response.getCode();
            if (statusCode == 200 || statusCode == 201) {
                // Parse the response body
                String responseBody = EntityUtils.toString(response.getEntity());
                return extractStatementId(responseBody);
            } else {
                throw new IOException("Failed to execute query. HTTP error code: " + statusCode);
            }
        }
    }

    public static String extractStatementId(String jsonResponse) throws IOException {
        // Parse the JSON response into a JsonNode object
        JsonNode rootNode = objectMapper.readTree(jsonResponse);

        // Extract the "statement_id" field
        JsonNode statementIdNode = rootNode.path("statement_id");

        // Check if the field exists and return its value
        if (!statementIdNode.isMissingNode()) {
            return statementIdNode.asText();
        } else {
            throw new IllegalArgumentException("statement_id not found in the response");
        }
    }
}
