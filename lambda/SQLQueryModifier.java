import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLQueryModifier {

    // Method to append filters to SQL query
    public static String appendFiltersToQuery(String originalQuery, String newFilters) {
        String lowerCaseQuery = originalQuery.toLowerCase();
        String whereClause = " WHERE ";

        // Regular expressions to match `GROUP BY`, `ORDER BY`, and other clauses.
        Pattern groupByPattern = Pattern.compile("\\sGROUP\\s+BY\\s", Pattern.CASE_INSENSITIVE);
        Pattern orderByPattern = Pattern.compile("\\sORDER\\s+BY\\s", Pattern.CASE_INSENSITIVE);
        Pattern joinPattern = Pattern.compile("\\sJOIN\\s", Pattern.CASE_INSENSITIVE);

        // Check if there's an existing WHERE clause in the SQL query
        if (lowerCaseQuery.contains(" where ")) {
            // If WHERE clause exists, we append using AND
            originalQuery = originalQuery.replaceFirst("(?i)where", "WHERE");
            whereClause = " AND ";
        } else {
            // If there's no WHERE clause, find the first instance of GROUP BY or ORDER BY to insert the filters
            Matcher groupByMatcher = groupByPattern.matcher(originalQuery);
            Matcher orderByMatcher = orderByPattern.matcher(originalQuery);
            Matcher joinMatcher = joinPattern.matcher(originalQuery);
            
            int insertionPoint = originalQuery.length(); // Default to the end of the query

            // Check if there's a GROUP BY clause
            if (groupByMatcher.find()) {
                insertionPoint = groupByMatcher.start();
            }

            // Check if there's an ORDER BY clause and adjust the insertion point
            if (orderByMatcher.find() && orderByMatcher.start() < insertionPoint) {
                insertionPoint = orderByMatcher.start();
            }

            // Check if there's a JOIN clause and adjust the insertion point
            if (joinMatcher.find() && joinMatcher.start() < insertionPoint) {
                insertionPoint = joinMatcher.start();
            }

            // If we find a place to insert the WHERE clause, do so
            originalQuery = new StringBuilder(originalQuery)
                    .insert(insertionPoint, " WHERE " + newFilters)
                    .toString();

            return originalQuery;
        }

        // Append the new filters to the existing WHERE clause
        return originalQuery + whereClause + newFilters;
    }

    public static void main(String[] args) {
        // Example SQL query
        String sqlQuery = "SELECT * FROM employees e JOIN departments d ON e.department_id = d.id ORDER BY e.name";
        
        // Example filter to append
        String newFilters = "e.salary > 50000 AND d.location = 'New York'";

        // Append filters to SQL query
        String modifiedQuery = appendFiltersToQuery(sqlQuery, newFilters);
        
        // Print the modified query
        System.out.println("Original SQL Query: ");
        System.out.println(sqlQuery);
        System.out.println("\nModified SQL Query: ");
        System.out.println(modifiedQuery);
    }
}
