package com.dtstack.flinkx.exception;

/**
 * 〈一句话功能简述〉<br> 
 * 〈数据异常〉
 *
 * @author wangnayu
 * @date 2020/5/7
 * @since 1.0.0
 */
public class DataErrorException extends RuntimeException {

    public DataErrorException(String message) {
        super(message);
    }
}
