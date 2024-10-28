import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLQueryModifier {

    // Method to append filters to SQL query
    public static String appendFiltersToQuery(String originalQuery, String newFilters) {
        String lowerCaseQuery = originalQuery.toLowerCase();
        StringBuilder modifiedQuery = new StringBuilder(originalQuery);
        
        // Set the default clause to `WHERE`
        String whereClause = " WHERE ";

        // Patterns to detect `JOIN`, `GROUP BY`, and `ORDER BY` clauses
        Pattern joinPattern = Pattern.compile("\\sJOIN\\s", Pattern.CASE_INSENSITIVE);
        Pattern groupByOrderByPattern = Pattern.compile("\\s(GROUP\\s+BY|ORDER\\s+BY)\\s", Pattern.CASE_INSENSITIVE);

        int insertionPoint = modifiedQuery.length(); // Default to the end of the query

        // Check for GROUP BY or ORDER BY to set an initial insertion point
        Matcher groupByOrderByMatcher = groupByOrderByPattern.matcher(originalQuery);
        if (groupByOrderByMatcher.find()) {
            insertionPoint = groupByOrderByMatcher.start();
        }

        // Ensure insertion point is after all JOIN clauses, if any exist
        Matcher joinMatcher = joinPattern.matcher(originalQuery);
        while (joinMatcher.find()) {
            insertionPoint = Math.max(insertionPoint, joinMatcher.end());
        }

        // Check if there's an existing WHERE clause
        if (lowerCaseQuery.contains(" where ")) {
            whereClause = " AND ";
        }

        // Insert the filters at the calculated insertion point
        modifiedQuery.insert(insertionPoint, whereClause + newFilters + " ");

        return modifiedQuery.toString();
    }
}
