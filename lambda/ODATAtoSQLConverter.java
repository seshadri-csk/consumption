import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.expression.*;
import org.apache.olingo.odata2.api.uri.expression.BinaryOperator;
import org.apache.olingo.odata2.api.uri.expression.ExpressionParserException;

public class ODataToSQLConverter {

    // This method maps OData filters to SQL equivalent
    public static String convertODataFilterToSQL(String odataFilter) throws ExpressionParserException {
        // Step 1: Parse OData filter string into expression tree
        FilterExpression filterExpression = UriParser.parseFilter(null, odataFilter);

        // Step 2: Traverse the expression tree and convert to SQL WHERE clause
        return traverseExpression(filterExpression.getExpression());
    }

    // This recursive method converts each part of the expression tree into SQL syntax
    public static String traverseExpression(CommonExpression expression) {
        if (expression instanceof BinaryExpression) {
            // Binary expressions (AND, OR, eq, lt, gt, etc.)
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            String left = traverseExpression(binaryExpression.getLeftOperand());
            String right = traverseExpression(binaryExpression.getRightOperand());
            BinaryOperator operator = binaryExpression.getOperator();
            
            // Map OData operators to SQL
            switch (operator) {
                case EQ: return left + " = " + right;
                case NE: return left + " != " + right;
                case LT: return left + " < " + right;
                case LE: return left + " <= " + right;
                case GT: return left + " > " + right;
                case GE: return left + " >= " + right;
                case AND: return left + " AND " + right;
                case OR: return left + " OR " + right;
                case IN: return left + " IN (" + right + ")";
                default: throw new UnsupportedOperationException("Unsupported operator: " + operator);
            }
        } else if (expression instanceof MethodExpression) {
            // Method expressions (endswith, etc.)
            MethodExpression methodExpression = (MethodExpression) expression;
            String param1 = traverseExpression(methodExpression.getParameters().get(0));
            String param2 = traverseExpression(methodExpression.getParameters().get(1));

            // Handle specific OData methods
            switch (methodExpression.getMethod()) {
                case ENDSWITH:
                    return param1 + " LIKE '%" + param2 + "'";
                default:
                    throw new UnsupportedOperationException("Unsupported method: " + methodExpression.getMethod());
            }
        } else if (expression instanceof MemberExpression) {
            // Simple member expressions (Name, Price, etc.)
            return ((MemberExpression) expression).getProperty().getUriLiteral();
        } else if (expression instanceof LiteralExpression) {
            // Literal values ('Milk', 2.55, etc.)
            return ((LiteralExpression) expression).getUriLiteral();
        }

        return "";
    }

    public static void main(String[] args) {
        try {
            // OData filter string
            String odataFilter = "Name in ('Milk', 'Cheese') AND style has Sales.Pattern'Yellow' "
                               + "AND ship eq 'Milk' or Price lt 2.55 AND product not endswith(Name,'ilk')";

            // Convert OData filter to SQL WHERE clause
            String sqlWhereClause = convertODataFilterToSQL(odataFilter);
            System.out.println("SQL WHERE Clause: " + sqlWhereClause);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
