public class ODataQueryValidator {

    // Basic method to validate OData query syntax
    public static boolean isValidODataQuery(String query) {
        // Regex patterns for SQL injection
        String[] forbiddenPatterns = { 
            "--", 
            ";", 
            "\\*", 
            "DROP", 
            "INSERT", 
            "DELETE", 
            "1=1", 
            "OR ", 
            "AND ", 
            "UNION ", 
            "xp_", 
            "/*", 
            "*/", 
            "CHAR(", 
            "EXEC", 
            "CAST(", 
            "CONVERT(" 
        };

        // Check for forbidden patterns (excluding valid OData single quotes)
        for (String pattern : forbiddenPatterns) {
            if (query.toUpperCase().contains(pattern.toUpperCase()) 
                && !pattern.equals("'")) {  // Allow single quotes for OData
                System.out.println("Potential SQL injection detected with pattern: " + pattern);
                return false;
            }
        }

        // Basic structure validation using a more relaxed regex
        // Allow alphanumeric characters, spaces, single quotes, and common OData operators
        // Updated regex to allow valid OData syntax, including function calls, slashes, and spaces
        String validCharactersRegex = "[\\w\\s=><'(),./-]+";
        if (!query.matches(validCharactersRegex)) {
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
