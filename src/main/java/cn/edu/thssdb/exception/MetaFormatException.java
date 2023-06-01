package cn.edu.thssdb.exception;

//元数据格式不正确
public class MetaFormatException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: Meta data format error!";
    }
}