package org.example.fraud.utils;

import java.lang.reflect.Field;
import java.math.BigDecimal;

public class FieldsExtractor {

    public static String getFieldAsString(Object object, String fieldName)
            throws IllegalAccessException, NoSuchFieldException {
        Class cls = object.getClass();
        Field field = cls.getField(fieldName);
        return field.get(object).toString();
    }

    public static BigDecimal getBigDecimalByName(String fieldName, Object object)
            throws IllegalAccessException, NoSuchFieldException {
        Class cls = object.getClass();
        Field field = cls.getField(fieldName);
        return new BigDecimal(field.get(object).toString());
    }
}
