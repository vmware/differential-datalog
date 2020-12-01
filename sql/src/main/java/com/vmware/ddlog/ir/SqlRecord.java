package com.vmware.ddlog.ir;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * This class is a simplified form of DDlogRecord optimized for SQL datatypes only.
 */
public class SqlRecord {
    public final String constructor;
    private final Object[] data;
    private int index;
    public final byte[] encodings;

    SqlRecord(String constructor, int capacity, byte[] encodings) {
        this.constructor = constructor;
        this.data = new Object[capacity];
        this.encodings = encodings.clone();
        this.index = 0;
        if (capacity > Byte.MAX_VALUE)
            throw new RuntimeException("Record too large: " + capacity);
    }

    protected static String getString(ByteBuffer buffer, int size) {
        String s = new String(buffer.array(), buffer.position(), size);
        buffer.position(buffer.position() + size);
        return s;
    }

    public SqlRecord(byte[] data) {
        ByteBuffer bs = ByteBuffer.wrap(data);
        if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
            bs.order(ByteOrder.BIG_ENDIAN);
        else
            bs.order(ByteOrder.LITTLE_ENDIAN);
        int capacity = bs.getInt();
        this.data = new Object[capacity];
        this.encodings = new byte[capacity];
        bs.get(this.encodings);
        int size = bs.getInt();
        this.constructor = getString(bs, size);
        for (int i = 0; i < capacity; i++) {
            byte enc = this.encodings[i];
            if ((enc & DDlogTStruct.Kind.Null.encoding) != 0) {
                this.data[i] = null;
                continue;
            }
            enc &= 0x3F;
            if (enc == DDlogTStruct.Kind.Bool.encoding) {
                this.data[i] = bs.get() == 1;
            } else if (enc == DDlogTStruct.Kind.Long.encoding) {
                this.data[i] = bs.getLong();
            } else if (enc == DDlogTStruct.Kind.String.encoding) {
                size = bs.getInt();
                this.data[i] = getString(bs, size);
            } else {
                throw new RuntimeException("Unexpected encoding: 0x" + Integer.toHexString(this.encodings[i]));
            }
        }
        if (bs.position() != bs.capacity())
            throw new RuntimeException("Not all data consumed");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlRecord sqlRecord = (SqlRecord) o;
        return constructor.equals(sqlRecord.constructor) &&
                Arrays.equals(data, sqlRecord.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(constructor);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    public int capacity() {
        return this.data.length;
    }

    private void checkNull(@Nullable Object o) {
        if (o == null)
            this.encodings[this.index] |= DDlogTStruct.Kind.Null.encoding;
    }

    public void add(@Nullable Object i) {
        this.checkNull(i);
        this.data[this.index++] = i;
    }

    public byte[] getEncoding() {
        if (this.index != this.capacity())
            throw new RuntimeException("Object has " + this.capacity() + " size, but only " +
                    this.index + " fields are present");
        // First pass: compute buffer size
        int size = 0;
        size += 4; // capacity
        size += this.encodings.length;
        byte[] constructor = this.constructor.getBytes();
        byte[][] bufs = new byte[this.capacity()][];
        size += 4 + constructor.length;
        for (int i = 0; i < this.data.length; i++) {
            Object o = this.data[i];
            if (o == null)
                continue;
            byte enc = (byte)(this.encodings[i] & 0x3F);
            if (enc == DDlogTStruct.Kind.Bool.encoding) {
                size += 1;
            } else if (enc == DDlogTStruct.Kind.Long.encoding) {
                size += 8;
            } else if (enc == DDlogTStruct.Kind.String.encoding) {
                bufs[i] = ((String)o).getBytes();
                size += 4 + bufs[i].length;
            } else {
                throw new RuntimeException("Unhandled type " + o.getClass());
            }
        }

        ByteBuffer bb = ByteBuffer.allocate(size);
        if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN))
            bb.order(ByteOrder.BIG_ENDIAN);
        else
            bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(this.capacity());
        bb.put(this.encodings);
        bb.putInt(constructor.length);
        bb.put(constructor);
        for (int i = 0; i < this.data.length; i++) {
            Object o = this.data[i];
            byte enc = (byte)(this.encodings[i] & 0x3F);
            if (o == null) {
                assert (this.encodings[i] & DDlogTStruct.Kind.Nullable.encoding) != 0;
                this.encodings[i] |= DDlogTStruct.Kind.Null.encoding;
                continue;
            }
            if (enc == DDlogTStruct.Kind.Bool.encoding) {
                bb.put((byte)((Boolean)o ? 1 : 0));
            } else if (enc == DDlogTStruct.Kind.Long.encoding) {
                bb.putLong((long)o);
            } else if (enc == DDlogTStruct.Kind.String.encoding) {
                bb.putInt(bufs[i].length);
                bb.put(bufs[i]);
            } else {
                throw new RuntimeException("Unhandled type " + o.getClass());
            }
        }
        return bb.array();
    }

    @Nullable
    public Object getData(int index) {
        return this.data[index];
    }

    public DDlogRecord createRecord() throws DDlogException {
        byte[] encoding = this.getEncoding();
        System.out.println();
        long handle = DDlogAPI.ddlog_create_sql_record(encoding);
        return DDlogRecord.fromHandle(handle);
    }
}
