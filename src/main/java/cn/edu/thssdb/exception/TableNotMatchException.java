package cn.edu.thssdb.exception;

public class TableNotMatchException extends RuntimeException {
  private String key;

  public TableNotMatchException(String key) {
    this.key = key;
  }

  public TableNotMatchException() {
    this.key = null;
  }

  @Override
  public String getMessage() {
    if (key != null) return "Exception: table " + key + " not match! ";
    else return "Exception: table not match!";
  }
}
