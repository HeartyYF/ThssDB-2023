package cn.edu.thssdb.exception;

// 从文件中恢复数据时出现错误
public class RecoverDataException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Internal error when recovering data!";
  }
}
