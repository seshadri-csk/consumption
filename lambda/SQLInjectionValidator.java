import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLInjectionValidator {
    // Corrected regular expression to detect SQL injection patterns
    private static final String SQL_INJECTION_PATTERN =
            ".*(\\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION|WHERE|OR|AND|LIKE|CAST|CONVERT|EXEC|EXECUTE|FROM|HAVING|JOIN|NULL|TRUE|FALSE|IS|IN)\\b|--|;|\\b\\w*\\s*=\\s*'.*?'|[^=]\\s*'.*').*";

    private static final Pattern pattern = Pattern.compile(SQL_INJECTION_PATTERN, Pattern.CASE_INSENSITIVE);

    /**
     * Validates the input string for SQL injection patterns.
     *
     * @param input The OData input string to validate.
     * @return true if the input is safe, false if it contains SQL injection patterns.
     */
    public static boolean isValidODataQuery(String input) {
        // Check if the input matches the SQL injection pattern
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            // Match found, return false
            int start = matcher.start(1); // Start index of the match
            int end = matcher.end(1); // End index of the match
            System.out.println("Potential SQL injection detected!");
            System.out.println("Matched Pattern: \"" + matcher.group(1) + "\" at indices: " + start + " to " + end);
            return false;
        }
        // No match found, return true
        return true;
    }

    public static void main(String[] args) {
        // Valid OData query
        String validOdataQuery = "runmode.sql eq 'production' and time_stamap ge to_date('01/10/2023','MM/DD/YY')";
        validateODataQuery(validOdataQuery);

        // Invalid OData query with SQL comment syntax
        String invalidOdataQueryWithComment = "runmode.sql eq 'production' -- and time_stamap ge to_date('01/10/2023','MM/DD/YY')";
        validateODataQuery(invalidOdataQueryWithComment);
        
        // Invalid OData query with SQL injection attempt
        String anotherInvalidOdataQuery = "runmode.sql eq 'production' OR 1=1; DROP TABLE users;";
        validateODataQuery(anotherInvalidOdataQuery);
    }

    /**
     * Validate and print the result for the provided OData query.
     *
     * @param odataQuery The OData query string to validate.
     */
    private static void validateODataQuery(String odataQuery) {
        // Validate the OData query
        if (isValidODataQuery(odataQuery)) {
            System.out.println("The OData query is valid and safe: \"" + odataQuery + "\"");
        } else {
            System.out.println("The OData query contains potential SQL injection patterns: \"" + odataQuery + "\"");
        }
    }
}
