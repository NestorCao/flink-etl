package com.dtstack.flinkx.exception;

import com.dtstack.flinkx.enums.ErrorType;

/**
 * 〈一句话功能简述〉<br> 
 * 〈字段长度错误异常〉
 *
 * @author wangnayu
 * @date 2020/5/19
 * @since 1.0.0
 */
public class FieldLengthErrorException extends ConversionException {

    public FieldLengthErrorException(String message, String errorField) {
        super(message, errorField, ErrorType.LENGTH_ERROR.getCode());
    }
}