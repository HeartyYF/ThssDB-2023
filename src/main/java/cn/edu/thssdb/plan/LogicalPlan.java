package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;
  protected String msg;
  protected long sessionId;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
    this.msg = null;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public String getMsg() {
    return msg;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setSessionId(long sessionId) {
    this.sessionId = sessionId;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DB,
    DROP_DB,
    CREATE_TABLE,
    SWITCH_DB,
    DROP_TABLE,
    SHOW_TABLE,
    INSERT,
    SELECT,
    DELETE,
    UPDATE,
    BEGIN_TRANSACTION,
    COMMIT,
    ROLLBACK,
    SAVEPOINT,
    CHECKPOINT,
  }

  public void exec() {}

  public void undo() {}
}
