import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.UriInfo;
import org.apache.olingo.odata2.api.uri.expression.ExpressionParserException;
import org.apache.olingo.odata2.core.uri.UriParserImpl;
import org.apache.olingo.odata2.api.uri.UriSyntaxException;

public class ODataQueryValidator {

    // Method to validate and parse OData query
    public static boolean isValidODataQuery(String odataQuery) {
        try {
            // Initialize Olingo's UriParser
            UriParser uriParser = new UriParserImpl();

            // Attempt to parse the OData query string
            UriInfo uriInfo = uriParser.parseUri(odataQuery, null, null);

            // If the query is valid, return true
            return true;
        } catch (UriSyntaxException | ExpressionParserException e) {
            // Handle exceptions: invalid or malicious OData query
            System.out.println("Invalid OData query detected: " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {
        // Example valid OData query
        String validOdataQuery = "runmode eq 'production' and time_stamap ge to_date('01/10/2023','MM/DD/YY')";
        boolean isValid = isValidODataQuery(validOdataQuery);
        System.out.println("Is valid OData query: " + isValid);

        // Example invalid OData query with potential SQL injection
        String invalidOdataQuery = "runmode eq 'production' -- DROP TABLE users;";
        boolean isInvalid = isValidODataQuery(invalidOdataQuery);
        System.out.println("Is valid OData query: " + isInvalid);
    }
}
