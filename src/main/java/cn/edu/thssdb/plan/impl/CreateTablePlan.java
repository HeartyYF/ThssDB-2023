package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DuplicateColumnException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;

import java.util.List;

public class CreateTablePlan extends LogicalPlan {
  private String tableName;
  private List<Column> columns;

  public CreateTablePlan(String tableName, List<Column> columns) {
    super(LogicalPlanType.CREATE_TABLE);
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "CreateTablePlan{" + "tableName='" + tableName + '\'' + ", columns=" + columns + '}';
  }

  @Override
  public void exec() {
    for (int i = 0; i < columns.size(); i++) {
      for (int j = 0; j < i; j++) {
        if (columns.get(i).getColumnName().equals(columns.get(j).getColumnName())) {
          throw new DuplicateColumnException(
              "Duplicate column name: " + columns.get(i).getColumnName() + "in table " + tableName);
        }
      }
    }
    Manager.getInstance()
        .getCurrentDatabase()
        .create(tableName, columns.toArray(new Column[columns.size()]));
  }
}
