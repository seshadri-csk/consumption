import java.util.List;
import java.util.Map;

public class MultiChunkDataConverter {

    public enum OutputFormat {
        JSON, CSV
    }

    // Converts chunk data to the specified format based on the schema
    public static String convertToFormat(List<Map<String, Object>> schema, List<List<Object>> dataArray, OutputFormat format) {
        if (format == OutputFormat.JSON) {
            return convertToJson(schema, dataArray);
        } else {
            return convertToCsv(schema, dataArray);
        }
    }

    // Converts data to JSON format
    private static String convertToJson(List<Map<String, Object>> schema, List<List<Object>> dataArray) {
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("[\n");

        for (List<Object> row : dataArray) {
            jsonBuilder.append("  {");
            for (int i = 0; i < schema.size(); i++) {
                Map<String, Object> column = schema.get(i);
                String columnName = (String) column.get("name");
                Object value = row.get(i);
                jsonBuilder.append("\"").append(columnName).append("\": ").append(value != null ? "\"" + value + "\"" : "null");
                if (i < schema.size() - 1) {
                    jsonBuilder.append(", ");
                }
            }
            jsonBuilder.append("},\n");
        }
        jsonBuilder.append("]");
        return jsonBuilder.toString();
    }

    // Converts data to CSV format
    private static String convertToCsv(List<Map<String, Object>> schema, List<List<Object>> dataArray) {
        StringBuilder csvBuilder = new StringBuilder();

        // Add headers
        for (int i = 0; i < schema.size(); i++) {
            Map<String, Object> column = schema.get(i);
            String columnName = (String) column.get("name");
            csvBuilder.append(columnName);
            if (i < schema.size() - 1) {
                csvBuilder.append(",");
            }
        }
        csvBuilder.append("\n");

        // Add data rows
        for (List<Object> row : dataArray) {
            for (int i = 0; i < row.size(); i++) {
                csvBuilder.append(row.get(i) != null ? row.get(i).toString() : "");
                if (i < row.size() - 1) {
                    csvBuilder.append(",");
                }
            }
            csvBuilder.append("\n");
        }

        return csvBuilder.toString();
    }
}
