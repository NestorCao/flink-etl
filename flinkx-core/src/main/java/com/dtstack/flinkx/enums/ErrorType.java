package com.dtstack.flinkx.enums;

/**
 * ClassName: ErrorType <br/>
 * Description: <br/>
 * date: 2020/5/21 13:10<br/>
 *
 * @author Zhen Guanqi<br/>
 * @since JDK 1.8
 */
public enum ErrorType {
    NULL_ERROR(1, "字段为空"),
    LENGTH_ERROR(2,"字段长度错误"),
    NOT_MATCH_ERROR(3, "字段不匹配"),
    ;

    private Integer code;
    private String value;

    ErrorType(int code, String value) {
        this.code = code;
        this.value = value;
    }

    public Integer getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }
}
