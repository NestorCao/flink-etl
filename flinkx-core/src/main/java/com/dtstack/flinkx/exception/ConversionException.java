package com.dtstack.flinkx.exception;

import lombok.Data;

/**
 * ClassName: ConversionException <br/>
 * Description: <br/>
 * date: 2020/5/20 17:08<br/>
 *
 * @author Zhen Guanqi<br/>
 * @since JDK 1.8
 */
@Data
public class ConversionException extends Exception{

    private Integer errorType;
    private String errorField;

    public ConversionException(String message, String errorField, Integer errorType) {
        super(message);
        this.errorType = errorType;
        this.errorField = errorField;
    }
}
