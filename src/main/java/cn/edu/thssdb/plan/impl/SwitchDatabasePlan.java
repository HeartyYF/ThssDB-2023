package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Manager;

public class SwitchDatabasePlan extends LogicalPlan {
  private String databaseName;

  public SwitchDatabasePlan(String databaseName) {
    super(LogicalPlanType.SWITCH_DB);
    this.databaseName = databaseName;
  }

  @Override
  public String toString() {
    return "SwitchDatabasePlan{" + "databaseName='" + databaseName + '\'' + '}';
  }

  @Override
  public void exec() {
    Manager.getInstance().switchDatabase(databaseName, sessionId);
    this.msg = "switch database into " + databaseName;
  }
}
