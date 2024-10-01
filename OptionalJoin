import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

public class QueryBuilder {

    public static String buildQuery(Map<String, Object> jsonMap) {
        StringBuilder queryBuilder = new StringBuilder("SELECT * FROM ");
        
        // Assuming the main table name is in jsonMap
        String tableName = (String) jsonMap.get("tableName");
        queryBuilder.append(tableName);

        // Handle joins with Optional
        Optional.ofNullable((List<Map<String, Object>>) jsonMap.get("joins"))
                .ifPresent(joins -> joins.forEach(join -> {
                    String joinType = Optional.ofNullable((String) join.get("joinType")).orElse("INNER");
                    String joinTable = (String) join.get("tableName");
                    Map<String, String> onClause = (Map<String, String>) join.get("on");

                    // Handle the join conditions using Optional to avoid null pointer issues
                    Optional<String> leftColumn = Optional.ofNullable(onClause.get("leftColumn"));
                    Optional<String> rightColumn = Optional.ofNullable(onClause.get("rightColumn"));

                    if (leftColumn.isPresent() && rightColumn.isPresent()) {
                        queryBuilder.append(" ")
                                .append(joinType)
                                .append(" JOIN ")
                                .append(joinTable)
                                .append(" ON ")
                                .append(leftColumn.get())
                                .append(" = ")
                                .append(rightColumn.get());
                    }
                }));

        return queryBuilder.toString();
    }

    public static void main(String[] args) {
        // Sample data
        Map<String, Object> jsonMap = Map.of(
            "tableName", "orders",
            "joins", List.of(
                Map.of("joinType", "LEFT", "tableName", "customers", "on", Map.of("leftColumn", "orders.customer_id", "rightColumn", "customers.id")),
                Map.of("joinType", "INNER", "tableName", "products", "on", Map.of("leftColumn", "orders.product_id", "rightColumn", "products.id"))
            )
        );

        // Build the query
        String sqlQuery = buildQuery(jsonMap);
        System.out.println(sqlQuery);
    }
}
