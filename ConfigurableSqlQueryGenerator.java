import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.*;

public class ConfigurableSqlQueryGenerator {

    // Configuration loaded from the config.json file
    private static Map<String, Object> config;

    public static void main(String[] args) throws Exception {
        // Load the configuration file
        ObjectMapper mapper = new ObjectMapper();
        config = mapper.readValue(new File("config.json"), Map.class);

        // Sample JSON input
        String jsonInput = "{\n" +
                "    \"tableName\": \"sales\",\n" +
                "    \"columns\": [\n" +
                "        {\"name\": \"product_name\", \"aggregation\": null},\n" +
                "        {\"name\": \"total_sales\", \"aggregation\": \"SUM\"},\n" +
                "        {\"name\": \"sales_date\", \"aggregation\": null},\n" +
                "        {\"name\": \"region\", \"aggregation\": null}\n" +
                "    ],\n" +
                "    \"joins\": [\n" +
                "        {\n" +
                "            \"joinType\": \"INNER\",\n" +
                "            \"tableName\": \"regions\",\n" +
                "            \"on\": {\n" +
                "                \"leftColumn\": \"sales.region_id\",\n" +
                "                \"rightColumn\": \"regions.id\"\n" +
                "            }\n" +
                "        }\n" +
                "    ],\n" +
                "    \"filters\": {\n" +
                "        \"AND\": [\n" +
                "            {\n" +
                "                \"dateRange\": {\n" +
                "                    \"column\": \"sales_date\",\n" +
                "                    \"startDate\": \"2024-01-01\",\n" +
                "                    \"endDate\": \"2024-12-31\"\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"OR\": [\n" +
                "                    {\"productCategory\": \"Electronics\"},\n" +
                "                    {\"region\": \"North America\"}\n" +
                "                ]\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"groupBy\": [\"product_name\", \"region\"],\n" +
                "    \"orderBy\": {\n" +
                "        \"column\": \"total_sales\",\n" +
                "        \"direction\": \"DESC\"\n" +
                "    },\n" +
                "    \"scheduledExpression\": \"rate(1 day)\"\n" +
                "}";

        // Parse the JSON input into a Map
        Map<String, Object> jsonMap = mapper.readValue(jsonInput, Map.class);

        // Generate SQL Query
        String sqlQuery = generateSqlQuery(jsonMap);
        System.out.println("Generated SQL Query:");
        System.out.println(sqlQuery);
    }

    /**
     * Generates a dynamic SQL query based on the input JSON.
     */
    public static String generateSqlQuery(Map<String, Object> jsonMap) {
        try {
            validateInput(jsonMap);
        } catch (IllegalArgumentException e) {
            System.err.println("Validation error: " + e.getMessage());
            // Use default query from configuration
            String defaultQuery = (String) config.get("defaultQuery");
            System.out.println("Using default query from configuration.");
            return defaultQuery;
        }

        StringBuilder queryBuilder = new StringBuilder();

        // Extract the table name and columns
        String tableName = (String) jsonMap.get("tableName");
        List<Map<String, String>> columns = (List<Map<String, String>>) jsonMap.get("columns");

        // Build the SELECT part
        StringJoiner columnJoiner = new StringJoiner(", ");
        for (Map<String, String> column : columns) {
            String columnName = column.get("name");
            String aggregation = column.get("aggregation");
            if (aggregation != null) {
                columnJoiner.add(aggregation + "(" + columnName + ")");
            } else {
                columnJoiner.add(columnName);
            }
        }

        queryBuilder.append("SELECT ").append(columnJoiner.toString()).append(" FROM ").append(tableName);

        // Handle Joins (if provided)
        List<Map<String, Object>> joins = (List<Map<String, Object>>) jsonMap.get("joins");
        if (joins != null && !joins.isEmpty()) {
            for (Map<String, Object> join : joins) {
                String joinType = (String) join.get("joinType");
                String joinTable = (String) join.get("tableName");
                Map<String, String> onClause = (Map<String, String>) join.get("on");

                String leftColumn = onClause.get("leftColumn");
                String rightColumn = onClause.get("rightColumn");

                queryBuilder.append(" ").append(joinType).append(" JOIN ").append(joinTable)
                        .append(" ON ").append(leftColumn).append(" = ").append(rightColumn);
            }
        }

        // Build the WHERE clause (if filters are provided)
        Map<String, Object> filters = (Map<String, Object>) jsonMap.get("filters");
        if (filters != null && !filters.isEmpty()) {
            String whereClause = buildWhereClause(filters);
            queryBuilder.append(" WHERE ").append(whereClause);
        }

        // Build GROUP BY clause (if provided)
        List<String> groupBy = (List<String>) jsonMap.get("groupBy");
        if (groupBy != null && !groupBy.isEmpty()) {
            StringJoiner groupByJoiner = new StringJoiner(", ");
            for (String group : groupBy) {
                groupByJoiner.add(group);
            }
            queryBuilder.append(" GROUP BY ").append(groupByJoiner.toString());
        }

        // Build ORDER BY clause (if provided)
        Map<String, String> orderBy = (Map<String, String>) jsonMap.get("orderBy");
        if (orderBy != null) {
            String orderColumn = orderBy.get("column");
            String orderDirection = orderBy.get("direction");
            queryBuilder.append(" ORDER BY ").append(orderColumn).append(" ").append(orderDirection);
        }

        // Handle scheduled expression (optional, just for logging/reporting)
        String scheduledExpression = (String) jsonMap.get("scheduledExpression");
        if (scheduledExpression != null) {
            System.out.println("Scheduled Expression: " + scheduledExpression);
        }

        // Return the final query
        return queryBuilder.toString();
    }

    /**
     * Builds the WHERE clause based on the provided filters.
     * Supports AND, OR, and date ranges.
     */
    private static String buildWhereClause(Map<String, Object> filters) {
        return buildCondition(filters, "AND");
    }

    /**
     * Recursively builds SQL conditions (AND/OR) for filtering.
     */
    private static String buildCondition(Map<String, Object> condition, String parentConditionType) {
        StringJoiner conditionJoiner = new StringJoiner(" " + parentConditionType + " ");

        for (String key : condition.keySet()) {
            Object value = condition.get(key);

            if (key.equals("AND") || key.equals("OR")) {
                List<Map<String, Object>> subConditions = (List<Map<String, Object>>) value;
                StringJoiner subConditionJoiner = new StringJoiner(" " + key + " ");
                for (Map<String, Object> subCondition : subConditions) {
                    subConditionJoiner.add(buildCondition(subCondition, key));
                }
                conditionJoiner.add("(" + subConditionJoiner.toString() + ")");
            } else if (key.equals("dateRange")) {
                Map<String, String> dateRange = (Map<String, String>) value;
                String dateColumn = dateRange.get("column");
                String startDate = dateRange.get("startDate");
                String endDate = dateRange.get("endDate");
                conditionJoiner.add(dateColumn + " BETWEEN '" + startDate + "' AND '" + endDate + "'");
            } else {
                conditionJoiner.add(key + " = '" + value + "'");
            }
        }

        return conditionJoiner.toString();
    }

    /**
     * Validates the input JSON against the configuration.
     */
    private static void validateInput(Map<String, Object> jsonMap) throws IllegalArgumentException {
        // Validate table name
        String tableName = (String) jsonMap.get("tableName");
        Map<String, Object> tables = (Map<String, Object>) config.get("tables");
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table '" + tableName + "' is not available.");
        }

        // Validate columns
        Map<String, Object> tableColumns = (Map<String, Object>) ((Map<String, Object>) tables.get(tableName)).get("columns");
        List<Map<String, String>> columns = (List<Map<String, String>>) jsonMap.get("columns");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be specified.");
        }

        for (Map<String, String> column : columns) {
            String columnName = column.get("name");
            String aggregation = column.get("aggregation");

            // Check if the column exists in the table
            if (!tableColumns.containsKey(columnName)) {
                throw new IllegalArgumentException("Column '" + columnName + "' does not exist in table '" + tableName + "'.");
            }

            // Check if the aggregation is supported for the column
            if (aggregation != null) {
                List<String> supportedAggregations = (List<String>) ((Map<String, Object>) tableColumns.get(columnName)).get("aggregations");
                if (!supportedAggregations.contains(aggregation)) {
                    throw new IllegalArgumentException("Aggregation '" + aggregation + "' is not supported for column '" + columnName + "'.");
                }
            }
        }

        // Validate joins
        List<Map<String, Object>> joins = (List<Map<String, Object>>) jsonMap.get("joins");
        if (joins != null) {
            for (Map<String, Object> join : joins) {
                String joinTableName = (String) join.get("tableName");

                // Check if the join table exists
                if (!tables.containsKey(joinTableName)) {
                    throw new IllegalArgumentException("Join table '" + joinTableName + "' is not available.");
                }

                // Validate join columns
                Map<String, String> onClause = (Map<String, String>) join.get("on");
                String leftColumn = onClause.get("leftColumn");
                String rightColumn = onClause.get("rightColumn");

                // Extract table and column names
                String[] leftParts = leftColumn.split("\\.");
                String[] rightParts = rightColumn.split("\\.");

                if (leftParts.length != 2 || rightParts.length != 2) {
                    throw new IllegalArgumentException("Invalid join column format.");
                }

                String leftTable = leftParts[0];
                String leftCol = leftParts[1];
                String rightTable = rightParts[0];
                String rightCol = rightParts[1];

                // Validate left column
                if (!tables.containsKey(leftTable) || !((Map<String, Object>) ((Map<String, Object>) tables.get(leftTable)).get("columns")).containsKey(leftCol)) {
                    throw new IllegalArgumentException("Column '" + leftCol + "' does not exist in table '" + leftTable + "'.");
                }

                // Validate right column
                if (!tables.containsKey(rightTable) || !((Map<String, Object>) ((Map<String, Object>) tables.get(rightTable)).get("columns")).containsKey(rightCol)) {
                    throw new IllegalArgumentException("Column '" + rightCol + "' does not exist in table '" + rightTable + "'.");
                }
            }
        }

        // Additional validations can be added here (e.g., for filters, groupBy, orderBy)
    }
}
