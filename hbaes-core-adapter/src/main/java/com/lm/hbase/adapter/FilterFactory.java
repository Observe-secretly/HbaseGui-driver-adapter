package com.lm.hbase.adapter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterFactory implements FilterFactoryInterface {

    public List<Filter> filterConvert(List<Object> filters) {
        List<Filter> result = new ArrayList<>();

        for (Object item : filters) {
            if (item instanceof PrefixFilter) {
                result.add((PrefixFilter) item);
            } else if (item instanceof SingleColumnValueFilter) {
                result.add((SingleColumnValueFilter) item);
            }
        }
        return result;

    }

    /**
     * 获取所有的过滤器类型
     * 
     * @return
     */
    public List<Class> getAllComparatorClass() {
        List<Class> result = new ArrayList<>();
        result.add(SubstringComparator.class);
        result.add(BinaryPrefixComparator.class);
        result.add(RegexStringComparator.class);
        return result;
    }

    /**
     * 获取操作符<br>
     * 在createSingleColumnValueFilter方法中，需要把getCompareOpSimpleList返回的操作符转换成CompareOp对象(如果它在其他版本还存在的话)
     * 
     * @return
     */
    public List<String> getCompareOpSimpleList() {
        List<String> list = new ArrayList<String>();
        list.add("=");
        list.add(">");
        list.add("<");
        list.add("≥");
        list.add("≤");
        list.add("≠");
        return list;
    }

    public Object createRowkeyPrefixFilter(byte[] rowkey) {
        return new PrefixFilter(rowkey);

    }

    /**
     * @param family
     * @param qualifier
     * @param compareOpSimple
     * @param fieldType 参考解析写法<br>
     * <p>
     * private byte[] filedValue(String type, String v) { try {<br>
     * &nbsp;switch (type.toLowerCase()) {<br>
     * &nbsp;&nbsp;case "string":<br>
     * &nbsp;return Bytes.toBytes(v);<br>
     * &nbsp;case "int":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(Integer.parseInt(v));<br>
     * &nbsp;case "short":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(Short.parseShort(v));<br>
     * &nbsp;case "long":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(Long.parseLong(v));<br>
     * &nbsp;case "float":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(Float.parseFloat(v));<br>
     * &nbsp;case "double":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(Double.parseDouble(v));<br>
     * &nbsp;case "bigdecimal":<br>
     * &nbsp;&nbsp;return Bytes.toBytes(new BigDecimal(v));<br>
     * &nbsp;default:<br>
     * &nbsp;&nbsp;return Bytes.toBytes(v);<br>
     * }<br>
     * } catch (Exception e) {<br>
     * return Bytes.toBytes(v);<br>
     * }<br>
     * }<br>
     * </p>
     * @param fieldValue
     * @return
     */
    public Object createSingleColumnValueFilter(byte[] family, byte[] qualifier, String compareOpSimple,
                                                String comparatorClassName, String fieldType, String fieldValue) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier, getCompareOp(compareOpSimple),
                                                                     getComparator(fieldType, fieldValue,
                                                                                   comparatorClassName));
        return filter;

    }

    /**
     * 根据选择操作转换成枚举
     * 
     * @return
     */
    private CompareOp getCompareOp(String operator) {

        switch (operator) {
            case "=":
                return CompareOp.EQUAL;
            case ">":
                return CompareOp.GREATER;
            case "<":
                return CompareOp.LESS;
            case "≥":
                return CompareOp.GREATER_OR_EQUAL;
            case "≤":
                return CompareOp.LESS_OR_EQUAL;
            case "≠":
                return CompareOp.NOT_EQUAL;

            default:
                return null;
        }

    }

    private ByteArrayComparable getComparator(String fieldType, String filedValue, String comparatorClassName) {

        if (comparatorClassName.toLowerCase().endsWith(BinaryPrefixComparator.class.getSimpleName().toLowerCase())) {// 前缀比较器
            return new BinaryPrefixComparator(convertValue(fieldType, filedValue));
        } else
            if (comparatorClassName.toLowerCase().endsWith(SubstringComparator.class.getSimpleName().toLowerCase())) {// 字串比较器
                return new SubstringComparator(filedValue);
            } else
                if (comparatorClassName.toLowerCase().endsWith(RegexStringComparator.class.getSimpleName().toLowerCase())) {// 支持正则
                    return new RegexStringComparator(filedValue);
                }
        return null;
    }

    private byte[] convertValue(String filedType, String filedValue) {

        try {
            switch (filedType.toLowerCase()) {
                case "string":
                    return Bytes.toBytes(filedValue);
                case "int":
                    return Bytes.toBytes(Integer.parseInt(filedValue));
                case "short":
                    return Bytes.toBytes(Short.parseShort(filedValue));
                case "long":
                    return Bytes.toBytes(Long.parseLong(filedValue));
                case "float":
                    return Bytes.toBytes(Float.parseFloat(filedValue));
                case "double":
                    return Bytes.toBytes(Double.parseDouble(filedValue));
                case "bigdecimal":
                    return Bytes.toBytes(new BigDecimal(filedValue));
                case "boolean":
                    return Bytes.toBytes(Boolean.parseBoolean(filedValue));
                default:
                    return Bytes.toBytes(filedValue);
            }

        } catch (Exception e) {
            return Bytes.toBytes(filedValue);
        }

    }

}
