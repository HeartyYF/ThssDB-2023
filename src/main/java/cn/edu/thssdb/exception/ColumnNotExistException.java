package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException {
  private String key;

  public ColumnNotExistException() {
    super();
    this.key = null;
  }

  public ColumnNotExistException(String key) {
    super();
    this.key = key;
  }

  @Override
  public String getMessage() {
    if (key == null) return "Exception: column doesn't exist!";
    else return "Exception: column \"" + this.key + "\" doesn't exist!";
  }
}
