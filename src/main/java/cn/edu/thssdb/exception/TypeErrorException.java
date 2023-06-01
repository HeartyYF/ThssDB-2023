package cn.edu.thssdb.exception;

// 数据类型不合法
public class TypeErrorException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: illegal data type";
    }
}
