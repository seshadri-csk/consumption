public class ODataToSQLConverter {

    // Convert OData query to SQL WHERE clause
    public static String convertODataToSQL(String odataQuery) {
        // Mapping OData operators to SQL equivalents
        String[][] odataToSqlMapping = {
            {" eq ", " = "},
            {" ne ", " != "},
            {" gt ", " > "},
            {" ge ", " >= "},
            {" lt ", " < "},
            {" le ", " <= "},
            {" and ", " AND "},
            {" or ", " OR "},
            {" not ", " NOT "},
            {"null", " IS NULL"},
            {"to_date", "TO_DATE"}  // OData to SQL function mapping
        };

        // First, handle the string functions (contains, startswith, endswith)
        odataQuery = handleStringFunctions(odataQuery);

        // Then, iterate through the mapping and replace OData operators with SQL equivalents
        String sqlQuery = odataQuery;
        for (String[] map : odataToSqlMapping) {
            sqlQuery = sqlQuery.replace(map[0], map[1]);
        }

        return sqlQuery;
    }

    // Handle OData string functions like startswith(), endswith(), and contains()
    private static String handleStringFunctions(String query) {
        // Handle startswith function: startswith(Property, Value) -> Property LIKE 'Value%'
        query = query.replaceAll("startswith\\((\\w+),\\s*'([^']+)'\\)", "$1 LIKE '$2%'");

        // Handle endswith function: endswith(Property, Value) -> Property LIKE '%Value'
        query = query.replaceAll("endswith\\((\\w+),\\s*'([^']+)'\\)", "$1 LIKE '%$2'");

        // Handle contains function: contains(Property, Value) -> Property LIKE '%Value%'
        query = query.replaceAll("contains\\((\\w+),\\s*'([^']+)'\\)", "$1 LIKE '%$2%'");

        return query;
    }

    public static void main(String[] args) {
        // Example complex OData query
        String odataQuery = "runmode eq 'production' and timestamp ge to_date('01/10/2023','MM/DD/YY') " +
                            "and contains(name, 'Milk') and startswith(style, 'Sale') or endswith(category, 'Fresh')";

        // Convert OData query to SQL format
        String sqlQuery = convertODataToSQL(odataQuery);
        System.out.println("SQL Query: " + sqlQuery);
    }
}
