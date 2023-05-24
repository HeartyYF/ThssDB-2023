package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.InsertErrorException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.*;

import java.util.ArrayList;

public class InsertPlan extends LogicalPlan {
  private String tableName;
  private ArrayList<String> columnNames = null;
  private ArrayList<ArrayList<String>> values;
  private ArrayList<Row> rowsHasInsert;
  private ArrayList<Row> rowsToInsert;
  private int[] columnMatch;
  private Table table;

  static final String errColumnNum =
      "Exception: insert operation error (columns don't match)!"; // 列数不匹配
  static final String errColumnType =
      "Exception: insert operation error (types don't match)!"; // 类型不匹配
  static final String errValueNum =
      "Exception:  insert operation error  (number of columns and values don't match)!"; // 列数与值数不匹配
  static final String errDuplicateValueType =
      "Exception: insert operation error  (duplicate name of columns)!"; // 列名重复
  static final String errColumnName =
      "Exception: insert operation error  (wrong column name)!"; // 属性名不在列定义中
  static final String errDuplicateKey =
      "Exception: insert operation error  (insertion causes duplicate key)!"; // 主键重复
  static final String errStringLength =
      "Exception: insert operation error  (string length exceeds the limit)!"; // 字符串过长

  public InsertPlan(
      String tableName, ArrayList<String> columnNames, ArrayList<ArrayList<String>> values) {
    super(LogicalPlanType.INSERT);
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.values = values;

    rowsHasInsert = new ArrayList<>();
    rowsToInsert = new ArrayList<>();
  }

  @Override
  public String toString() {
    String repo = "Rows that got inserted: ";
    for (Row row : rowsHasInsert) {
      repo += row.toString();
    }
    return repo;
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

    ArrayList<Column> columns = table.getColumns();

    int primaryKeyIndex = table.primaryIndex;
    String primaryKey = columns.get(primaryKeyIndex).getColumnName();

    if (columnNames == null) {
      for (ArrayList<String> value : values) {
        if (value.size() > columns.size()) {
          throw new InsertErrorException(errValueNum);
        }
        while (value.size() < columns.size()) {
          value.add(null);
        }
        ArrayList<Entry> entries = new ArrayList<>();

        // 类型检查
        for (int i = 0; i < columns.size(); i++) {
          matchType(columns.get(i), value.get(i), primaryKey, entries);
        }
        Row newRow = new Row(entries);
        rowsToInsert.add(newRow);
      }
    } else {
      columnMatch = new int[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        columnMatch[i] = -1;
      }
      if (columnNames.size() > columns.size()) {
        throw new InsertErrorException(errColumnNum + "1" + columns.size() + columnNames.size());
      }
      for (ArrayList<String> items : values) {
        if (items.size() != columnNames.size()) {
          throw new InsertErrorException(errColumnNum + "2" + columns.size() + columnNames.size());
        }
      }

      // 检查列名重复或不存在
      for (int i = 0; i < columnNames.size(); i++) {
        for (int j = 0; j < i; j++) {
          if (columnNames.get(i).equals(columnNames.get(j))) {
            throw new InsertErrorException(errDuplicateValueType);
          }
        }
        boolean hasMatched = false;
        for (int j = 0; j < columns.size(); j++) {
          if (columnNames.get(i).equals(table.getColumns().get(j).getColumnName())) {
            hasMatched = true;
            columnMatch[j] = i;
            break;
          }
        }
        if (hasMatched == false) {
          throw new InsertErrorException(errColumnName);
        }
      }

      for (ArrayList<String> value : values) {

        ArrayList<Entry> entries = new ArrayList<>();

        int i = 0;
        for (int j = 0; j < columns.size(); j++) {
          Column c = columns.get(j);
          int match = columnMatch[i];
          // 将没匹配到的列的值置为null
          if (match != -1) {
            matchType(c, value.get(match), primaryKey, entries);
          } else {
            if (c.isNotNull()) {
              throw new InsertErrorException(
                  "Exception: insert operation error ( column "
                      + c.getColumnName()
                      + " cannot be null )");
            } else {
              entries.add(new Entry(null));
            }
          }
          i++;
        }
        Row newRow = new Row(entries);
        rowsToInsert.add(newRow);
      }
    }
    insert();
  }
  //        public void undo() {
  //            return;
  //            for (Row row : rowsHasInsert) {
  //                table.delete(row);
  //            }
  //        }
  //
  //        public LinkedList<String> getLog() {
  //            return;
  //            LinkedList<String> log = new LinkedList<>();
  //            for (Row row : rowsHasInsert) {
  //                log.add("INSERT " + tableName + " " + row.toString());
  //            }
  //            return log;
  //        }

  private void insert() {
    try {
      for (Row row : rowsToInsert) {
        table.insert(row);
        rowsHasInsert.add(row);
      }
    } catch (Exception e) {
      //                undo();
      throw new InsertErrorException(errDuplicateKey);
    }

    rowsToInsert.clear();
  }

  // 将字符串转为相应的值加入entries
  private void matchType(Column column, String value, String primaryKey, ArrayList<Entry> entries) {

    if (value == null) {
      if (column.isNotNull()) {
        throw new InsertErrorException(
            "Exception:  insert operation error ( " + column.getColumnName() + " cannot be null)");
      } else {
        entries.add((new Entry(null)));
      }
      return;
    }

    switch (column.getColumnType()) {
      case INT:
        try {
          int tmp = Integer.parseInt(value);
          entries.add(new Entry(tmp));
        } catch (NumberFormatException e) {
          throw new InsertErrorException(errColumnType);
        }
        break;
      case LONG:
        try {
          long tmp = Long.parseLong(value);
          entries.add(new Entry(tmp));
        } catch (NumberFormatException e) {
          throw new InsertErrorException(errColumnType);
        }
        break;
      case DOUBLE:
        try {
          double tmp = Double.parseDouble(value);
          entries.add(new Entry(tmp));
        } catch (NumberFormatException e) {
          throw new InsertErrorException(errColumnType);
        }
        break;
      case FLOAT:
        try {
          float tmp = Float.parseFloat(value);
          entries.add(new Entry(tmp));
        } catch (NumberFormatException e) {
          throw new InsertErrorException(errColumnType);
        }
        break;
      case STRING:
        if (value.length() > column.getMaxLength()) {
          throw new InsertErrorException(errStringLength);
        }
        entries.add(new Entry(value));
    }
  }
}
