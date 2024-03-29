package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Manager;

public class DropDatabasePlan extends LogicalPlan {
  private String databaseName;

  public DropDatabasePlan(String databaseName) {
    super(LogicalPlanType.DROP_DB);
    this.databaseName = databaseName;
  }

  @Override
  public String toString() {
    return "DropDatabasePlan{" + "databaseName='" + databaseName + '\'' + '}';
  }

  @Override
  public void exec() {
    Manager.getInstance().deleteDatabase(databaseName);
    this.msg = "successfully drop database " + databaseName;
  }
}
