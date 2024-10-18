import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.apache.hc.core5.http.ParseException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;

public class DatabricksSyncClient {
    private final String databricksUrl;
    private final String authToken;

    public DatabricksSyncClient(String databricksUrl, String authToken) {
        this.databricksUrl = databricksUrl;
        this.authToken = authToken;
    }

    public String executeQuerySync(String query, int timeoutSeconds) throws IOException, ParseException {
        // Prepare the Databricks API payload
        JSONObject payload = new JSONObject();
        payload.put("query", query);
        payload.put("timeout", timeoutSeconds);

        // Create an HttpClient with a timeout
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionTimeToLive(Timeout.of(timeoutSeconds, TimeUnit.SECONDS))
                .build()) {

            // Build the HTTP POST request
            HttpPost postRequest = new HttpPost(databricksUrl + "/sql/execute");
            postRequest.setHeader("Authorization", "Bearer " + authToken);
            postRequest.setHeader("Content-Type", "application/json");
            postRequest.setEntity(new StringEntity(payload.toString()));

            // Execute the request
            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                int statusCode = response.getCode();
                String responseBody = new String(response.getEntity().getContent().readAllBytes());

                if (statusCode == 200) {
                    return responseBody;
                } else {
                    throw new RuntimeException("Databricks query failed with status code: " + statusCode);
                }
            }
        }
    }
}
