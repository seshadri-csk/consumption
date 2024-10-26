import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class ODataToSQLConverterTest {

    // Tests for isValidODataQuery
    @Test
    public void testIsValidODataQuery_NoInjection_ShouldReturnTrue() {
        String validQuery = "timestamp ge to_date('2024-01-01', 'yyyy-MM-dd') and id eq '123'";
        assertTrue(ODataToSQLConverter.isValidODataQuery(validQuery), "Expected valid query to return true");
    }

    @Test
    public void testIsValidODataQuery_SQLInjectionDetected_ShouldReturnFalse() {
        String injectionQuery = "name eq 'John' OR 1=1--";
        assertFalse(ODataToSQLConverter.isValidODataQuery(injectionQuery), "Expected SQL injection pattern to return false");
    }

    @Test
    public void testIsValidODataQuery_SQLInjectionSemicolon_ShouldReturnFalse() {
        String injectionQuery = "timestamp ge '2024-01-01'; DROP TABLE users;";
        assertFalse(ODataToSQLConverter.isValidODataQuery(injectionQuery), "Expected SQL injection pattern to return false for semicolon");
    }

    @Test
    public void testIsValidODataQuery_EmptyQuery_ShouldReturnTrue() {
        String emptyQuery = "";
        assertTrue(ODataToSQLConverter.isValidODataQuery(emptyQuery), "Expected empty query to return true");
    }

    // Tests for convertODataToSQL
    @Test
    public void testConvertODataToSQL_SimpleEqualConversion() {
        String odataQuery = "id eq '123'";
        String expectedSQL = "id = '123'";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_ODataFunctionsToSQL() {
        String odataQuery = "contains(name, 'John') and startswith(address, '123')";
        String expectedSQL = "name LIKE '%John%' AND address LIKE '123%'";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_ComparisonOperators() {
        String odataQuery = "price gt 100 and price le 500";
        String expectedSQL = "price > 100 AND price <= 500";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_NullComparison() {
        String odataQuery = "name eq null";
        String expectedSQL = "name IS NULL";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_ToDateFunctionMapping() {
        String odataQuery = "timestamp ge to_date('2024-10-01', 'yyyy-MM-dd')";
        String expectedSQL = "timestamp >= TO_DATE('2024-10-01', 'yyyy-MM-dd')";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    // Tests for handleStringFunctions (private method) indirectly through convertODataToSQL
    @Test
    public void testConvertODataToSQL_HandleStringFunctions_StartsWith() {
        String odataQuery = "startswith(name, 'John')";
        String expectedSQL = "name LIKE 'John%'";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_HandleStringFunctions_EndsWith() {
        String odataQuery = "endswith(name, 'Doe')";
        String expectedSQL = "name LIKE '%Doe'";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }

    @Test
    public void testConvertODataToSQL_HandleStringFunctions_Contains() {
        String odataQuery = "contains(name, 'Doe')";
        String expectedSQL = "name LIKE '%Doe%'";
        assertEquals(expectedSQL, ODataToSQLConverter.convertODataToSQL(odataQuery));
    }
}
