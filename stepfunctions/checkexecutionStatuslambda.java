public class CheckExecutionStatus implements RequestHandler<Map<String, Object>, Map<String, Object>> {
    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        Map<String, Object> response = new HashMap<>();
        // Logic to check if query is still running
        boolean isRunning = false; // Example status

        if (isRunning) {
            response.put("status", "RUNNING");
        } else {
            response.put("status", "COMPLETED");
        }

        return response;
    }
}
