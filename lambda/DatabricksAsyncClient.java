import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.json.JSONObject;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DatabricksAsyncClient {
    private final String databricksUrl;
    private final String authToken;

    public DatabricksAsyncClient(String databricksUrl, String authToken) {
        this.databricksUrl = databricksUrl;
        this.authToken = authToken;
    }

    public CompletableFuture<String> executeQueryAsync(String query) {
        // Prepare the Databricks API payload
        JSONObject payload = new JSONObject();
        payload.put("query", query);

        // Create an asynchronous HTTP client
        IOReactorConfig reactorConfig = IOReactorConfig.custom()
                .setSoTimeout(Timeout.ofSeconds(30))
                .build();

        try (CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
                .setIOReactorConfig(reactorConfig)
                .build()) {

            // Start the client
            httpClient.start();

            // Prepare the HTTP POST request
            BasicRequestProducer requestProducer = new BasicRequestProducer(
                    "POST", URI.create(databricksUrl + "/sql/execute"),
                    AsyncEntityProducer.create(payload.toString(), "application/json"));

            // Execute the asynchronous request
            CompletableFuture<String> responseFuture = new CompletableFuture<>();
            httpClient.execute(requestProducer, (HttpResponse response) -> {
                int statusCode = response.getCode();
                if (statusCode == 200) {
                    responseFuture.complete(response.getEntity().toString());
                } else {
                    responseFuture.completeExceptionally(new RuntimeException(
                            "Databricks query failed with status code: " + statusCode));
                }
            });

            return responseFuture;
        }
    }
}
