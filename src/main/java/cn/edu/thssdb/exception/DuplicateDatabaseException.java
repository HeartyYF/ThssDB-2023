package cn.edu.thssdb.exception;

public class DuplicateDatabaseException extends RuntimeException {
  @Override
  public String getMessage() {
    return "Exception: database already exists!";
  }
}
