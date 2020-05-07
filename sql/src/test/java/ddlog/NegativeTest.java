package ddlog;

import com.vmware.ddlog.translator.TranslationException;

import org.junit.Test;

public class NegativeTest extends BaseQueriesTest {
    @Test(expected = TranslationException.class)
    public void testMixAggregate() {
        String query = "create view v0 as select max(column1), column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate1() {
        String query = "create view v0 as select min(column1) + column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate2() {
        String query = "create view v0 as select min(min(column1)) from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate3() {
        String query = "create view v0 as select *, min(column1) from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testSelectStartGroupBy() {
        String query = "create view v0 as select *, min(column1) from t1 group by column2";
        this.testTranslation(query, "");
    }
}