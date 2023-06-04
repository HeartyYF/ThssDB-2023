package cn.edu.thssdb.exception;

public class AmbiguousColumnException extends RuntimeException {
  private String key;

  public AmbiguousColumnException() {
    super();
    this.key = null;
  }

  public AmbiguousColumnException(String key) {
    super();
    this.key = key;
  }

  @Override
  public String getMessage() {
    if (key == null) return "Exception: ambiguous column!";
    else return "Exception: column \"" + this.key + "\" is ambiguous!";
  }
}
