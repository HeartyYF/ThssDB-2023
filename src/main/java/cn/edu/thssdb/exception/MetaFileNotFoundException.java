package cn.edu.thssdb.exception;

// 没有找到元数据文件
public class MetaFileNotFoundException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: Meta file for this table is not found!";
  }
}
