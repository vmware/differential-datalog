package ddlog;

import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.translator.Translator;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SqlRecordTest extends BaseQueriesTest {
    @Test
    public void sqlRecordTest() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(true);
        byte[] encoding = rec.getEncoding();
        SqlRecord rec1 = new SqlRecord(encoding);
        Assert.assertEquals(rec.capacity(), rec1.capacity());
        Assert.assertEquals(rec.constructor, rec1.constructor);
        Assert.assertEquals("String", rec1.getData(0));
        Assert.assertEquals(2L, rec1.getData(1));
        Assert.assertEquals(true, rec1.getData(2));
    }

    @Test
    public void sqlRecordTestNullable() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance.setMayBeNull(true)));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64.setMayBeNull(true)));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance.setMayBeNull(true)));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(true);
        byte[] encoding = rec.getEncoding();
        SqlRecord rec1 = new SqlRecord(encoding);
        Assert.assertEquals(rec.capacity(), rec1.capacity());
        Assert.assertEquals(rec.constructor, rec1.constructor);
        Assert.assertEquals("String", rec1.getData(0));
        Assert.assertEquals(2L, rec1.getData(1));
        Assert.assertEquals(true, rec1.getData(2));
    }

    @Test(expected = RuntimeException.class)
    public void notEnoughFieldsTest() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.getEncoding();
    }

    @Test(expected = ClassCastException.class)
    public void wrongDataTest() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(3L);
        rec.getEncoding();
    }

    @Test
    public void sqlRecordTestNull() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance.setMayBeNull(true)));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64.setMayBeNull(true)));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add(null);
        rec.add(null);
        byte[] encoding = rec.getEncoding();
        SqlRecord rec1 = new SqlRecord(encoding);
        Assert.assertEquals(rec.capacity(), rec1.capacity());
        Assert.assertEquals(rec.constructor, rec1.constructor);
        Assert.assertNull(rec1.getData(0));
        Assert.assertNull(rec1.getData(1));
    }

    @Test
    public void sqlRecordTestSomeNull() {
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance.setMayBeNull(true)));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance.setMayBeNull(true)));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add(null);
        rec.add(3L);
        rec.add(null);
        byte[] encoding = rec.getEncoding();
        SqlRecord rec1 = new SqlRecord(encoding);
        Assert.assertEquals(rec.capacity(), rec1.capacity());
        Assert.assertEquals(rec.constructor, rec1.constructor);
        Assert.assertNull(rec1.getData(0));
        Assert.assertEquals(3L, rec1.getData(1));
        Assert.assertNull(rec1.getData(2));
    }

    @Test
    public void sqlToDDlogRecordTest() throws DDlogException {
        DDlogAPI.loadLibrary();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(true);
        DDlogRecord ddrec = rec.createRecord();
        Assert.assertNotNull(ddrec);
        Assert.assertTrue(ddrec.isStruct());
        Assert.assertEquals(rec.constructor, ddrec.getStructName());
        Assert.assertEquals(rec.getData(0), ddrec.getStructField(0).getString());
        Assert.assertEquals(rec.getData(1), ddrec.getStructField(1).getInt().longValue());
        Assert.assertEquals(rec.getData(2), ddrec.getStructField(2).getBoolean());
    }

    @Test
    public void sqlToDDlogRecordTestNullable() throws DDlogException {
        DDlogAPI.loadLibrary();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance.setMayBeNull(true)));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64.setMayBeNull(true)));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance.setMayBeNull(true)));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(true);
        DDlogRecord ddrec = rec.createRecord();
        Assert.assertNotNull(ddrec);
        Assert.assertTrue(ddrec.isStruct());
        Assert.assertEquals(rec.constructor, ddrec.getStructName());
        Assert.assertEquals(rec.getData(0), ddrec.getStructField(0).getStructField(0).getString());
        Assert.assertEquals(rec.getData(1), ddrec.getStructField(1).getStructField(0).getInt().longValue());
        Assert.assertEquals(rec.getData(2), ddrec.getStructField(2).getStructField(0).getBoolean());
    }

    void showByteArray(byte[] data) {
        for (byte b: data)
            System.out.print(Integer.toHexString(b) + " ");
        System.out.println();
    }

    @Test
    public void twoWayConversion() throws DDlogException {
        DDlogAPI.loadLibrary();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        fields.add(new DDlogField(null,"s", DDlogTString.instance));
        fields.add(new DDlogField(null,"x", DDlogTSigned.signed64));
        fields.add(new DDlogField(null,"b", DDlogTBool.instance));
        DDlogTStruct struct = new DDlogTStruct(null, "table", fields);
        SqlRecord rec = struct.createEmptyRecord();
        rec.add("String");
        rec.add(2L);
        rec.add(true);
        byte[] original = rec.getEncoding();
        DDlogRecord ddrec = rec.createRecord();
        byte[] encoding = DDlogAPI.ddlog_encode_record(ddrec.getHandleAndInvalidate());
        Assert.assertArrayEquals(original, encoding);
        SqlRecord rec1 = new SqlRecord(encoding);
        Assert.assertEquals(rec, rec1);
    }

    @Test
    public void createRecordFromRelation() {
        Translator t = this.createInputTables(false);
        DDlogProgram ddprogram = t.getDDlogProgram();
        DDlogRelationDeclaration decl = ddprogram.getRelation("Rt2");
        Assert.assertNotNull(decl);
        DDlogType type = decl.getType();
        DDlogTStruct ts = t.resolveType(type).to(DDlogTStruct.class);
        SqlRecord rec = ts.createEmptyRecord();
        rec.add(2L);
        Assert.assertNotNull(rec.getEncoding());
    }
}
