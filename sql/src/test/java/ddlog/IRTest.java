/*
 * Copyright (c) 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ddlog;

import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.ir.DDlogNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.ir.DDlogVisitor;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.PrestoSqlStatement;
import org.junit.Assert;
import org.junit.Test;

public class IRTest extends BaseQueriesTest {
    static class CountNodes extends DDlogVisitor {
        public int nodes = 0;
        public int expressions = 0;

        CountNodes() { super(true); }

        @Override
        public void postorder(DDlogExpression node) {
            this.expressions++;
            super.postorder(node);
        }

        @Override
        public void postorder(DDlogNode node) {
            this.nodes++;
        }
    }

    @Test
    public void visitorTest() {
        String query = "create view v as select distinct * from t1";
        Translator t = this.createInputTables(false);
        t.translateSqlStatement(new PrestoSqlStatement(query));
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        CountNodes visitor = new CountNodes();
        ddprogram.accept(visitor);
        Assert.assertEquals(52, visitor.nodes);
        Assert.assertEquals(3, visitor.expressions);
    }

    @Test
    public void unusedColumnTest() {
        String query = "create view v as select distinct t1.column1 as c1 from t1";
        Translator t = this.createInputTables(false);
        t.translateSqlStatement(new PrestoSqlStatement(query));
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        CountNodes visitor = new CountNodes();
        ddprogram.accept(visitor);
        Assert.assertEquals(62, visitor.nodes);
        Assert.assertEquals(6, visitor.expressions);
    }

    @Test
    public void unusedFilterTest() {
        String query = "create view v as select distinct t1.column1 as c1 from t1 where t1.column3";
        Translator t = this.createInputTables(false);
        t.translateSqlStatement(new PrestoSqlStatement(query));
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        CountNodes visitor = new CountNodes();
        ddprogram.accept(visitor);
        Assert.assertEquals(65, visitor.nodes);
        Assert.assertEquals(8, visitor.expressions);
    }
}
