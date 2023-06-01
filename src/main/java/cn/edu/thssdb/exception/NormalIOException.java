package cn.edu.thssdb.exception;

// IO错误
public class NormalIOException extends RuntimeException  {
    @Override
    public String getMessage() {
        return "Exception: An IOError occurred";
    }
}