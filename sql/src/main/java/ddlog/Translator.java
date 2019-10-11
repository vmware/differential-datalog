package ddlog;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;

/**
 * Hello world!
 *
 */
public class Translator
{
    private static final SqlParser PARSER = new SqlParser();

    public static void translateSql(final String sql) {
        final ParsingOptions options = new ParsingOptions();
        final Statement statement = PARSER.createStatement(sql, options);
        final ExampleVisitor visitor = new ExampleVisitor();
        visitor.process(statement);
    }

    static class ExampleVisitor extends DefaultTraversalVisitor<Void, Void> {
        @Override
        protected Void visitTable(final Table node, final Void context) {
            System.out.println("Hello node! " + node);
            return super.visitTable(node, context);
        }
    }
}
