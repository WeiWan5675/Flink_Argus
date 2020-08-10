package org.weiwan.argus.core.pub.pojo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/10 10:21
 * @Package: org.weiwan.argus.core.pub.pojo
 * @ClassName: DataRow
 * @Description:
 **/
@PublicEvolving
public class DataRow<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The array to store actual values.
     */
    private final T[] fields;

    /**
     * Create a new Row instance.
     *
     * @param arity The number of fields in the Row
     */
    public DataRow(int arity) {
        this.fields = (T[]) new Object[arity];
    }


    /**
     * Get the number of fields in the Row.
     *
     * @return The number of fields in the Row.
     */
    public int getArity() {
        return fields.length;
    }

    /**
     * Gets the field at the specified position.
     *
     * @param pos The position of the field, 0-based.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public Object getField(int pos) {
        return fields[pos];
    }

    /**
     * Sets the field at the specified position.
     *
     * @param pos   The position of the field, 0-based.
     * @param value The value to be assigned to the field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public void setField(int pos, T value) {
        fields[pos] = value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(StringUtils.arrayAwareToString(fields[i]));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataRow row = (DataRow) o;

        return Arrays.deepEquals(fields, row.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(fields);
    }

    /**
     * Creates a new Row and assigns the given values to the Row's fields.
     * This is more convenient than using the constructor.
     *
     * <p>For example:
     *
     * <pre>
     *     Row.of("hello", true, 1L);}
     * </pre>
     * instead of
     * <pre>
     *     Row row = new Row(3);
     *     row.setField(0, "hello");
     *     row.setField(1, true);
     *     row.setField(2, 1L);
     * </pre>
     */
    public static DataRow of(DataField... values) {
        DataRow row = new DataRow(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }

    /**
     * Creates a new Row which copied from another row.
     * This method does not perform a deep copy.
     *
     * @param row The row being copied.
     * @return The cloned new Row
     */
    public static DataRow copy(DataRow row) {
        final DataRow newRow = new DataRow(row.fields.length);
        System.arraycopy(row.fields, 0, newRow.fields, 0, row.fields.length);
        return newRow;
    }

    /**
     * Creates a new Row with projected fields from another row.
     * This method does not perform a deep copy.
     *
     * @param fields fields to be projected
     * @return the new projected Row
     */
    public static DataRow project(DataRow row, int[] fields) {
        final DataRow newRow = new DataRow(fields.length);
        for (int i = 0; i < fields.length; i++) {
            newRow.fields[i] = row.fields[fields[i]];
        }
        return newRow;
    }
}
