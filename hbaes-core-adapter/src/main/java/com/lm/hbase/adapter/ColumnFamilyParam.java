package com.lm.hbase.adapter;

import java.util.HashMap;

import com.lm.hbase.adapter.ColumnFamilyParam.ColumnFamilyFieldEnum;

public class ColumnFamilyParam extends HashMap<ColumnFamilyFieldEnum, Object> {

    private static final long serialVersionUID = -2899892867939377399L;

    public static enum ColumnFamilyFieldEnum {

                                              COLUMN_FAMILY_NAME, TIME_TO_LIVE, MAX_VERSION;

    }

}
