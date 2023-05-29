package cn.edu.thssdb.exception;

public class DatabaseInUseException extends Exception {
  private String key;

  public DatabaseInUseException() {
    super();
    this.key = null;
  }

  public DatabaseInUseException(String key) {
    super();
    this.key = key;
  }

  @Override
  public String getMessage() {
    if (key == null) return "Exception: database in use!";
    else return "Exception: database \"" + this.key + "\" in use!";
  }
}
