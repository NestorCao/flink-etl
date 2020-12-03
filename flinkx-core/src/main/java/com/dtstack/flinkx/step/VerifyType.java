package com.dtstack.flinkx.step;

import java.time.format.DateTimeFormatter;

public enum VerifyType {
    STRING{
        @Override
        public boolean verify(Object val, Object... params) {

            return true;
        }
    },
    INTEGER{
        @Override
        public boolean verify(Object val, Object... params) {
            try {
                Long.parseLong(val.toString());
            } catch (Exception e) {
                return false;
            }
            return true;
        }
    },
    DOUBLE{
        @Override
        public boolean verify(Object val, Object... params) {
            try {
                Double.parseDouble(val.toString());
            } catch (Exception e) {
                return false;
            }
            return true;
        }
    },
    DATATIME{
        @Override
        public boolean verify(Object val, Object... params) {
            try {
                DateTimeFormatter.ofPattern(params[0].toString()).parse(val.toString());
            } catch (Exception e) {
                return false;
            }
            return true;
        }
    };

    public abstract boolean verify(Object val, Object... params);
}
