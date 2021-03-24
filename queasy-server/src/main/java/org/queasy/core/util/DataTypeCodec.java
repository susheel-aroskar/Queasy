package org.queasy.core.util;

import static java.sql.Types.*;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class DataTypeCodec {

    public static Object fromString(final int sqlType, final String s) {
        switch (sqlType) {
            case BIT: return Boolean.valueOf(s);
            case TINYINT: return Byte.valueOf(s);
            case SMALLINT: return Short.valueOf(s);
            case INTEGER: return Integer.valueOf(s);
            case BIGINT: return Long.valueOf(s);
            case FLOAT: return  Float.valueOf(s);
            case REAL:
            case DOUBLE: return Double.valueOf(s);
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR: return s;
            default: throw new IllegalArgumentException("No mapping found for type java.sql.Types "+sqlType);
        }
    }
}
