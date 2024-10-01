import java.util.*;
import java.util.stream.Collectors;

public class ReportLambda {

    /**
     * Builds the WHERE clause based on the provided filters.
     * Supports AND, OR, and date ranges.
     */
    private static String buildWhereClause(Map<String, Object> filters) {
        return buildCondition(filters, "AND").orElse("");
    }

    /**
     * Recursively builds SQL conditions (AND/OR) for filtering using Optional.
     */
    private static Optional<String> buildCondition(Map<String, Object> condition, String parentConditionType) {
        StringJoiner conditionJoiner = new StringJoiner(" " + parentConditionType + " ");

        for (String key : condition.keySet()) {
            Object value = condition.get(key);

            if (key.equals("AND") || key.equals("OR")) {
                List<Map<String, Object>> subConditions = (List<Map<String, Object>>) value;
                StringJoiner subConditionJoiner = new StringJoiner(" " + key + " ");

                subConditions.forEach(subCondition ->
                    buildCondition(subCondition, key).ifPresent(subConditionJoiner::add)
                );

                // Add the joined subcondition to the main condition joiner
                if (subConditionJoiner.length() > 0) {
                    conditionJoiner.add("(" + subConditionJoiner.toString() + ")");
                }
            } else if (key.equals("dateRange")) {
                Optional<String> dateRangeCondition = buildDateRangeCondition((Map<String, String>) value);
                dateRangeCondition.ifPresent(conditionJoiner::add);
            } else {
                conditionJoiner.add(key + " = '" + value + "'");
            }
        }

        return conditionJoiner.length() > 0 ? Optional.of(conditionJoiner.toString()) : Optional.empty();
    }

    /**
     * Builds the date range condition for the WHERE clause.
     */
    private static Optional<String> buildDateRangeCondition(Map<String, String> dateRange) {
        return Optional.ofNullable(dateRange)
            .map(range -> {
                String dateColumn = range.get("column");
                String startDate = range.get("startDate");
                String endDate = range.get("endDate");

                // Both start and end dates are required to build the condition
                if (dateColumn != null && startDate != null && endDate != null) {
                    return dateColumn + " BETWEEN '" + startDate + "' AND '" + endDate + "'";
                }
                return null;
            });
    }

    public static void main(String[] args) {
        // Example input
        Map<String, Object> filters = new HashMap<>();
        filters.put("AND", Arrays.asList(
            Map.of("productCategory", "Electronics"),
            Map.of("OR", Arrays.asList(
                Map.of("region", "North"),
                Map.of("region", "South")
            )),
            Map.of("dateRange", Map.of("column", "sales_date", "startDate", "2024-01-01", "endDate", "2024-12-31"))
        ));

        // Generate the WHERE clause
        String whereClause = buildWhereClause(filters);
        System.out.println(whereClause);
    }
}
