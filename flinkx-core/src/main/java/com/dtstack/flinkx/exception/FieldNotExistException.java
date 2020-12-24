package com.dtstack.flinkx.exception;

import com.dtstack.flinkx.enums.ErrorType;

/**
 * 〈一句话功能简述〉<br> 
 * 〈字段不存在异常〉
 *
 * @author wangnayu
 * @date 2020/5/8
 * @since 1.0.0
 */
public class FieldNotExistException extends ConversionException{

    public FieldNotExistException(String message, String errorField) {
        super(message, errorField, ErrorType.NULL_ERROR.getCode());
    }
}