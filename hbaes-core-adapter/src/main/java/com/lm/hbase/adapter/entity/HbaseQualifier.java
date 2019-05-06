package com.lm.hbase.adapter.entity;

import java.nio.charset.Charset;

/**
 * 类QueryTab.java的实现描述：存放Hbase修饰列的详细信息<br>
 * 包括修饰列的String类型展示属性、原始类型byte[]数组和修饰列的类型
 * 
 * @author limin Mar 13, 2019 2:57:09 AM
 */
public class HbaseQualifier {

    public static final String  UTF8_ENCODING = "UTF-8";

    public static final Charset UTF8_CHARSET  = Charset.forName(UTF8_ENCODING);

    String                      displayName;
    byte[]                      family;
    byte[]                      qualifier;
    String                      type;

    public HbaseQualifier(byte[] family, byte[] qualifier, String type){
        this.family = family;
        this.qualifier = qualifier;
        this.type = type;
        this.displayName = byteToString(family, 0, family.length) + "." + byteToString(qualifier, 0, qualifier.length);

    }

    public static String byteToString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return this.displayName;
    }

}
