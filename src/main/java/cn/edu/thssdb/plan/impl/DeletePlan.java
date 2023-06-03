package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DeletePlan extends LogicalPlan {
  private String tableName;
  private ArrayList<String> whereCondition;
  private ArrayList<Row> rowsHasDelete;
  private Table table;

  public DeletePlan(String tableName, SQLParser.MultipleConditionContext mc) {
    super(LogicalPlanType.DELETE);
    this.tableName = tableName;
    this.whereCondition = new ArrayList<>();
    if (mc != null && mc.condition() != null) {
      whereCondition.add(mc.condition().expression(0).getText());
      whereCondition.add(mc.condition().comparator().getText());
      whereCondition.add(mc.condition().expression(1).getText());
    } else {
      this.whereCondition = null;
    }
    rowsHasDelete = new ArrayList<>();
  }

  public void exec() {
    // 获取数据库和表
    Database db = Manager.getInstance().getCurrentDatabase();
    if (db == null) {
      throw new DatabaseNotExistException();
    }
    table = db.get(tableName);
    if (table == null) {
      throw new TableNotExistException();
    }

    Iterator<Row> rowIterator = table.iterator();
    if (whereCondition == null) {
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        rowsHasDelete.add(row);
      }
      table.delete();
    } else {
      ArrayList<Column> columns = table.getColumns();
      ArrayList<String> columnNames = new ArrayList<>();
      for (Column column : columns) {
        columnNames.add(column.getColumnName());
      }
      QueryTable qTable = new QueryTable(table, columnNames, whereCondition);
      qTable.execute();
      // 不知道为什么这里不能用iterator...
      List<Row> rows = qTable.resultRows;
      for (Row row : rows) {
        table.delete(row);
        rowsHasDelete.add(row);
      }
    }
  }

  @Override
  public void undo() {
    for (Row row : rowsHasDelete) {
      table.insert(row);
    }
  }

  @Override
  public String toString() {
    String repo = "Rows that got deleted: ";
    for (Row row : rowsHasDelete) {
      repo += row.toString();
    }
    return repo;
  }

  public String getTableName() {
    return tableName;
  }
}
