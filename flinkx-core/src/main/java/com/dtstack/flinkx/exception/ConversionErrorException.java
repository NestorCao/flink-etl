package com.dtstack.flinkx.exception;

/**
 * 〈一句话功能简述〉<br> 
 * 〈数据转换异常〉
 *
 * @author wangnayu
 * @date 2020/5/11
 * @since 1.0.0
 */
public class ConversionErrorException extends Exception {

    public ConversionErrorException(String message) {
        super(message);
    }
}