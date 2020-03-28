package ddlog;

import com.vmware.ddlog.translator.TranslationException;
import org.junit.Test;

public class WindowTest extends BaseQueriesTest {
    @Test(expected = TranslationException.class)
    public void windowTest() {
        String query = "create view v1 as\n" +
                "select *, count(*) over (partition by column2) as c2 from t1";
        String translation = "";
        this.testTranslation(query, translation);
    }
}
