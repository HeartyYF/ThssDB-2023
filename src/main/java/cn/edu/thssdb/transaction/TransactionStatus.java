package cn.edu.thssdb.transaction;

/*
事务状态类
*/
public class TransactionStatus {

  boolean status;
  String message;

  public TransactionStatus(boolean status, String message) {
    this.status = status;
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  public boolean getStatus() {
    return status;
  }
}
