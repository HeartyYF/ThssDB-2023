package cn.edu.thssdb.exception;

public class DuplicateDatabaseException extends RuntimeException {
  private String key;

  public DuplicateDatabaseException() {
    super();
    this.key = null;
  }

  public DuplicateDatabaseException(String key) {
    super();
    this.key = key;
  }

  @Override
  public String getMessage() {
    if (key == null) return "Exception: database already exists!";
    else return "Exception: database \"" + this.key + "\" already exists!";
  }
}
