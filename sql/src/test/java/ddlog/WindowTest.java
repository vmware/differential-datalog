package ddlog;

import org.junit.Test;

public class WindowTest extends BaseQueriesTest {
    @Test
    public void windowTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(column3) over (partition by column2) as c2 from t1";
        String translation = "";
        this.testTranslation(query, translation);
    }
}
