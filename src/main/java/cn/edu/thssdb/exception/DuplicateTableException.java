package cn.edu.thssdb.exception;

public class DuplicateTableException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: table already exists!";
  }
}
