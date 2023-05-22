package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException {

  private String msg;

  public DuplicateColumnException(String msg) {
    super();
    this.msg = msg;
  }

  @Override
  public String getMessage() {
    return "Exception: " + msg;
  }
}
