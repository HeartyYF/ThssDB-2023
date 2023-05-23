package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

import static cn.edu.thssdb.type.ColumnType.STRING;

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
    String str = "Show table " + tableName + "\n-----------------------------------\n";
    Database database = Manager.getInstance().getCurrentDatabase();
    if (database == null) {
      throw new DatabaseNotExistException();
    }
    Table table = database.get(tableName);
    for (int i = 0; i < table.columns.size(); i++) {
      Column column = table.columns.get(i);
      str +=
          " "
              + column.getColumnName()
              + " \t\t "
              + column.getColumnType()
              + (column.getColumnType() == STRING ? "(" + column.getMaxLength() + ")" : "")
              + " \t\t "
              + (column.isPrimary() ? "Primary Key" : "")
              + " \t\t "
              + (column.isNotNull() ? "Not Null" : "")
              + "\n";
    }
    str += "-----------------------------------\n";
    this.msg = str;
  }
}
