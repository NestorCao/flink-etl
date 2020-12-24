package com.dtstack.flinkx.exception;

import com.dtstack.flinkx.enums.ErrorType;

/**
 * 〈一句话功能简述〉<br> 
 * 〈字段正则验证异常〉
 *
 * @author wangnayu
 * @date 2020/5/19
 * @since 1.0.0
 */
public class FieldNotMatchesException extends ConversionException{

    public FieldNotMatchesException(String message, String errorField) {
        super(message, errorField, ErrorType.NOT_MATCH_ERROR.getCode());
    }
}