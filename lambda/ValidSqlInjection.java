import java.util.regex.Pattern;
import java.util.Set;
import java.util.HashSet;

public class ODataInputValidator {

    // Whitelist of valid OData operators and functions
    private static final Set<String> VALID_OPERATORS = new HashSet<>();
    
    static {
        VALID_OPERATORS.add("eq");
        VALID_OPERATORS.add("ne");
        VALID_OPERATORS.add("lt");
        VALID_OPERATORS.add("le");
        VALID_OPERATORS.add("gt");
        VALID_OPERATORS.add("ge");
        VALID_OPERATORS.add("and");
        VALID_OPERATORS.add("or");
        VALID_OPERATORS.add("in");
        VALID_OPERATORS.add("startswith");
        VALID_OPERATORS.add("endswith");
        VALID_OPERATORS.add("contains");
        VALID_OPERATORS.add("not");
        VALID_OPERATORS.add("has");
    }

    // Updated regular expression to allow valid SQL functions like to_date() with single quotes
// Updated regular expression to allow safe SQL expressions while blocking SQL injection attempts
private static final Pattern SQL_INJECTION_PATTERN1 = Pattern.compile(
    "(\\b(select|insert|delete|update|drop|union|--|\\/\\*|\\*\\/|;--|\\bor\\b|\\band\\b\\s[^\\w])|'\\s*;|'.*\\bOR\\b|'.*\\bAND\\b|'.*\\bSELECT\\b)",
    Pattern.CASE_INSENSITIVE
);

    // Regular expression to detect common SQL injection patterns
    private static final Pattern SQL_INJECTION_PATTERN = Pattern.compile(
        "([';--]|\\/\\*|\\*\\/|\\bor\\b|\\band\\b|\\bselect\\b|\\binsert\\b|\\bdelete\\b|\\bupdate\\b|\\bdrop\\b|\\bunion\\b|\\b--\\b)", 
        Pattern.CASE_INSENSITIVE
    );
    
    // Function to validate the OData filter string
    public static boolean validateODataFilter(String odataFilter) {
        // Check for SQL injection patterns
        if (SQL_INJECTION_PATTERN.matcher(odataFilter).find()) {
            return false;  // Detected possible SQL injection
        }

        // Check if the operators/functions used in the string are valid OData operators/functions
        String[] tokens = odataFilter.split("\\s+");
        for (String token : tokens) {
            if (token.matches("[a-zA-Z]+") && !VALID_OPERATORS.contains(token.toLowerCase())) {
                return false;  // Detected invalid operator/function
            }
        }

        return true;  // Input passed all checks
    }

    // Function to validate the OData $orderby clause
    public static boolean validateODataOrderBy(String odataOrderBy) {
        // Allow only field names followed by optional 'asc' or 'desc'
        return odataOrderBy.matches("^[a-zA-Z0-9_]+( asc| desc)?(,\\s*[a-zA-Z0-9_]+( asc| desc)?)*$");
    }

    public static void main(String[] args) {
        // Test inputs
        String validODataFilter = "Name eq 'Milk' or Price lt 2.55 and product not endswith(Name,'ilk')";
        String invalidODataFilter = "Name eq 'Milk'; DROP TABLE users; --";
        String validOrderBy = "Price asc, Name desc";
        String invalidOrderBy = "Price; DROP TABLE users";

        // Validate inputs
        System.out.println("Valid OData filter? " + validateODataFilter(validODataFilter));   // Should be true
        System.out.println("Valid OData filter? " + validateODataFilter(invalidODataFilter)); // Should be false

        System.out.println("Valid $orderby clause? " + validateODataOrderBy(validOrderBy));   // Should be true
        System.out.println("Valid $orderby clause? " + validateODataOrderBy(invalidOrderBy)); // Should be false
    }
}
