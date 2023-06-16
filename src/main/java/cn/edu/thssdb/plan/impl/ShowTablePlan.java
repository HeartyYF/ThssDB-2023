package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

public class ShowTablePlan extends LogicalPlan {
  private String tableName;

  public ShowTablePlan(String databaseName) {
    super(LogicalPlanType.SHOW_TABLE);
    this.tableName = databaseName;
  }

  @Override
  public String toString() {
    return "ShowTablePlan{" + "databaseName='" + tableName + '\'' + '}';
  }

  @Override
  public void exec() {
    String str = "Show table ";
    Database database = Manager.getInstance().getCurrentDatabase(sessionId);
    if (database == null) {
      throw new DatabaseNotExistException();
    }
    Table table = database.get(tableName);
    str += table.toString();
    this.msg = str;
  }

  public String getTableName() {
    return tableName;
  }
}
