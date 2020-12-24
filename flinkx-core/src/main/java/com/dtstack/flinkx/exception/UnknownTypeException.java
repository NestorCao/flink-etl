package com.dtstack.flinkx.exception;

/**
 * 〈一句话功能简述〉<br> 
 * 〈未知类型异常〉
 *
 * @author wangnayu
 * @date 2020/5/7
 * @since 1.0.0
 */
public class UnknownTypeException extends RuntimeException{

    public UnknownTypeException(String message) {
        super(message);
    }
}