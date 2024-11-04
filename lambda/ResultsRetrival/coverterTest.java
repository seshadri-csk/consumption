import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiChunkDataConverterTest {

    private List<Map<String, Object>> schema;
    private List<List<Object>> dataArray;

    @BeforeEach
    void setUp() {
        schema = List.of(
            Map.of("name", "id"),
            Map.of("name", "name"),
            Map.of("name", "age")
        );

        dataArray = List.of(
            List.of(1, "Alice", 30),
            List.of(2, "Bob", 25),
            List.of(3, "Charlie", null)
        );
    }

    @Test
    void testConvertToFormat_JSON() {
        String expectedJson = "[\n" +
            "  {\"id\": \"1\", \"name\": \"Alice\", \"age\": \"30\"},\n" +
            "  {\"id\": \"2\", \"name\": \"Bob\", \"age\": \"25\"},\n" +
            "  {\"id\": \"3\", \"name\": \"Charlie\", \"age\": null},\n" +
            "]";
        
        String jsonOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.JSON);
        assertEquals(expectedJson, jsonOutput.trim());
    }

    @Test
    void testConvertToFormat_CSV() {
        String expectedCsv = "id,name,age\n" +
            "1,Alice,30\n" +
            "2,Bob,25\n" +
            "3,Charlie,\n";
        
        String csvOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.CSV);
        assertEquals(expectedCsv, csvOutput.trim());
    }

    @Test
    void testConvertToFormat_EmptyData_JSON() {
        List<List<Object>> emptyDataArray = List.of();
        String expectedJson = "[\n]";
        
        String jsonOutput = MultiChunkDataConverter.convertToFormat(schema, emptyDataArray, MultiChunkDataConverter.OutputFormat.JSON);
        assertEquals(expectedJson, jsonOutput.trim());
    }

    @Test
    void testConvertToFormat_EmptyData_CSV() {
        List<List<Object>> emptyDataArray = List.of();
        String expectedCsv = "id,name,age\n";
        
        String csvOutput = MultiChunkDataConverter.convertToFormat(schema, emptyDataArray, MultiChunkDataConverter.OutputFormat.CSV);
        assertEquals(expectedCsv, csvOutput.trim());
    }

    @Test
    void testConvertToFormat_NullValues_JSON() {
        dataArray = List.of(
            List.of(null, "Alice", 30),
            List.of(2, null, 25),
            List.of(3, "Charlie", null)
        );
        String expectedJson = "[\n" +
            "  {\"id\": null, \"name\": \"Alice\", \"age\": \"30\"},\n" +
            "  {\"id\": \"2\", \"name\": null, \"age\": \"25\"},\n" +
            "  {\"id\": \"3\", \"name\": \"Charlie\", \"age\": null},\n" +
            "]";

        String jsonOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.JSON);
        assertEquals(expectedJson, jsonOutput.trim());
    }

    @Test
    void testConvertToFormat_NullValues_CSV() {
        dataArray = List.of(
            List.of(null, "Alice", 30),
            List.of(2, null, 25),
            List.of(3, "Charlie", null)
        );
        String expectedCsv = "id,name,age\n" +
            ",Alice,30\n" +
            "2,,25\n" +
            "3,Charlie,\n";

        String csvOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.CSV);
        assertEquals(expectedCsv, csvOutput.trim());
    }

    @Test
    void testConvertToFormat_SingleColumn_JSON() {
        schema = List.of(Map.of("name", "id"));
        dataArray = List.of(
            List.of(1),
            List.of(2),
            List.of(3)
        );
        String expectedJson = "[\n" +
            "  {\"id\": \"1\"},\n" +
            "  {\"id\": \"2\"},\n" +
            "  {\"id\": \"3\"},\n" +
            "]";

        String jsonOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.JSON);
        assertEquals(expectedJson, jsonOutput.trim());
    }

    @Test
    void testConvertToFormat_SingleColumn_CSV() {
        schema = List.of(Map.of("name", "id"));
        dataArray = List.of(
            List.of(1),
            List.of(2),
            List.of(3)
        );
        String expectedCsv = "id\n" +
            "1\n" +
            "2\n" +
            "3\n";

        String csvOutput = MultiChunkDataConverter.convertToFormat(schema, dataArray, MultiChunkDataConverter.OutputFormat.CSV);
        assertEquals(expectedCsv, csvOutput.trim());
    }
}
