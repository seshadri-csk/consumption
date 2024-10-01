import java.util.*;
import java.util.stream.Collectors;

public class ReportLambda {
    
    // This method generates the SQL based on input and uses Optional for joins, filters, and conditions
    public String generateSqlFromInput(Map<String, Object> input, Map<String, Object> config) {
        String tableName = Optional.ofNullable((String) input.get("tableName"))
                                   .orElseThrow(() -> new IllegalArgumentException("Missing tableName"));
        
        List<String> columns = Optional.ofNullable((List<String>) input.get("columns"))
                                       .orElseThrow(() -> new IllegalArgumentException("Missing columns"));
        
        List<Map<String, String>> joins = (List<Map<String, String>>) input.get("joins"); // Optional joins

        Map<String, Object> filters = (Map<String, Object>) input.get("filters");
        List<String> groupBy = (List<String>) input.get("groupBy");
        Map<String, String> orderBy = (Map<String, String>) input.get("orderBy");
        
        // Base query with columns
        StringBuilder sql = new StringBuilder("SELECT ")
            .append(columns.stream().collect(Collectors.joining(", ")))
            .append(" FROM ").append(tableName);

        // Append JOINs if available
        Optional.ofNullable(joins).ifPresent(j -> sql.append(" ").append(buildJoinClause(j, config)));

        // Add WHERE clause based on filters
        Optional.ofNullable(filters).ifPresent(f -> sql.append(" WHERE ").append(buildWhereClause(f)));

        // Add GROUP BY clause
        Optional.ofNullable(groupBy).ifPresent(g -> sql.append(" GROUP BY ").append(String.join(", ", g)));

        // Add ORDER BY clause
        Optional.ofNullable(orderBy).ifPresent(o -> sql.append(" ORDER BY ")
            .append(o.get("column"))
            .append(" ")
            .append(o.get("direction")));

        return sql.toString();
    }

    // Build JOIN clause dynamically from input and config
    private String buildJoinClause(List<Map<String, String>> joins, Map<String, Object> config) {
        return joins.stream()
                .map(join -> {
                    String joinTable = join.get("table");
                    String joinCondition = join.get("condition");
                    String joinType = Optional.ofNullable(join.get("type")).orElse("INNER"); // Default to INNER JOIN
                    return joinType + " JOIN " + joinTable + " ON " + joinCondition;
                })
                .collect(Collectors.joining(" "));
    }

    // Build WHERE clause with complex filtering including AND/OR conditions
    private String buildWhereClause(Map<String, Object> filters) {
        List<String> conditions = new ArrayList<>();

        // Example: Handle a date range filter
        Optional.ofNullable(filters.get("dateRange")).ifPresent(dateRange -> {
            Map<String, String> range = (Map<String, String>) dateRange;
            String column = range.get("column");
            String startDate = range.get("startDate");
            String endDate = range.get("endDate");
            conditions.add(column + " BETWEEN '" + startDate + "' AND '" + endDate + "'");
        });

        // Example: Handle product category
        Optional.ofNullable(filters.get("productCategory")).ifPresent(category -> {
            conditions.add("productCategory = '" + category + "'");
        });

        // More conditions can be added here similarly

        // Combine conditions using AND/OR
        return conditions.stream().collect(Collectors.joining(" AND "));
    }
}
