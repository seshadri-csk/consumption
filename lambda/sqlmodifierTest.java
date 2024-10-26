import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class SQLQueryModifierTest {

    @Test
    public void testAppendFiltersToQuery_NoWhereClause() {
        String originalQuery = "SELECT * FROM customers";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT * FROM customers WHERE age > 25";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_ExistingWhereClause() {
        String originalQuery = "SELECT * FROM customers WHERE country = 'US'";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT * FROM customers WHERE country = 'US' AND age > 25";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_WithGroupByClause() {
        String originalQuery = "SELECT age, COUNT(*) FROM customers GROUP BY age";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT age, COUNT(*) FROM customers WHERE age > 25 GROUP BY age";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_WithOrderByClause() {
        String originalQuery = "SELECT * FROM customers ORDER BY age";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT * FROM customers WHERE age > 25 ORDER BY age";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_WithJoinClause() {
        String originalQuery = "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id = orders.customer_id";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT customers.name, orders.amount FROM customers WHERE age > 25 JOIN orders ON customers.id = orders.customer_id";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_WithWhereAndOrderByClauses() {
        String originalQuery = "SELECT * FROM customers WHERE country = 'US' ORDER BY age";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT * FROM customers WHERE country = 'US' AND age > 25 ORDER BY age";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_WithMultipleClauses() {
        String originalQuery = "SELECT age, COUNT(*) FROM customers WHERE country = 'US' GROUP BY age ORDER BY age";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT age, COUNT(*) FROM customers WHERE country = 'US' AND age > 25 GROUP BY age ORDER BY age";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }

    @Test
    public void testAppendFiltersToQuery_CaseInsensitiveWhereClause() {
        String originalQuery = "SELECT * FROM customers WHERE country = 'US'";
        String newFilters = "age > 25";
        String expectedQuery = "SELECT * FROM customers WHERE country = 'US' AND age > 25";
        
        assertEquals(expectedQuery, SQLQueryModifier.appendFiltersToQuery(originalQuery, newFilters));
    }
}
