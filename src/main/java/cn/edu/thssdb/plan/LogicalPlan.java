package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;
  protected String msg;

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
  }

  public void exec() {}
}
