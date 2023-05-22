package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;

public class DropTablePlan extends LogicalPlan {
  private String tableName;

  public DropTablePlan(String tableName) {
    super(LogicalPlanType.DROP_TABLE);
    this.tableName = tableName;
  }

  @Override
  public String toString() {
    return "DropTablePlan{" + "tableName='" + tableName + '\'' + '}';
  }

  @Override
  public void exec() {
    Database database = Manager.getInstance().getCurrentDatabase();
    if (database == null) {
      throw new DatabaseNotExistException();
    }
    database.dropTable(this.tableName);
  }
}
