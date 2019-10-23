package ddlog;

import com.facebook.presto.sql.TreePrinter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;

import java.io.PrintStream;
import java.util.IdentityHashMap;

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
        System.out.println(statement.toString());
    }

    static class ExampleVisitor extends DefaultTraversalVisitor<Void, Void> {
        @Override
        protected Void visitTable(final Table node, final Void context) {
            System.out.println("Hello node! " + node);
            return super.visitTable(node, context);
        }
    }
}
