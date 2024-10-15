public class ODataQueryValidator {

    // Basic method to validate OData query syntax
    public static boolean isValidODataQuery(String query) {
        // Regex patterns for SQL injection
        String[] forbiddenPatterns = { "--", ";", "\\*", "DROP", "INSERT", "DELETE", "'", "\"", "xp_", "1=1", "OR", "AND", "UNION" };

        // Check for forbidden patterns
        for (String pattern : forbiddenPatterns) {
            if (query.toUpperCase().contains(pattern.toUpperCase())) {
                System.out.println("Potential SQL injection detected with pattern: " + pattern);
                return false;
            }
        }

        // Basic structure validation (simple example)
        if (!query.matches("[\\w\\s=><'(),.]+")) {
            System.out.println("Invalid characters detected in OData query.");
            return false;
        }

        // If no SQL injection patterns are found and structure looks valid
        return true;
    }

    public static void main(String[] args) {
        // Example valid OData query
        String validOdataQuery = "runmode eq 'production' and timestamp ge to_date('01/10/2023','MM/DD/YY')";
        boolean isValid = isValidODataQuery(validOdataQuery);
        System.out.println("Is valid OData query: " + isValid);

        // Example invalid OData query with SQL injection attempt
        String invalidOdataQuery = "runmode eq 'production' -- DROP TABLE users;";
        boolean isInvalid = isValidODataQuery(invalidOdataQuery);
        System.out.println("Is valid OData query: " + isInvalid);
    }
}
