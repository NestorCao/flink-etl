package com.dtstack.flinkx.exception;

/**
 * 〈一句话功能简述〉<br> 
 * 〈类型错误〉
 *
 * @author wangnayu
 * @date 2020/5/7
 * @since 1.0.0
 */
public class TypeErrorException extends RuntimeException{

    public TypeErrorException(String message) {
        super(message);
    }
}