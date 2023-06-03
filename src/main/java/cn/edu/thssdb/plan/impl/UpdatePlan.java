package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Pair;

import java.util.*;

public class UpdatePlan extends LogicalPlan {
  private String tableName;
  private String columnName;
  private String attrValue;
  private ArrayList<String> whereCondition;
  private ArrayList<Pair<Row, Row>> rowsHasUpdate;
  private int columnIdxToUpdate;
  private Table table = null;

  static final String errColumnName = "Exception: wrong update operation ( no such column )!";
  static final String errColumnType = "Exception: wrong update operation ( type unmatched )!";
  static final String duplicateKey =
      "Exception: wrong update operation ( update causes duplicate key )!";
  static final String errStringLength =
      "Exception: wrong update operation ( update causes string exceeds length limit )!";

  public UpdatePlan(
      String tableName,
      String columnNmae,
      String attrValue,
      SQLParser.MultipleConditionContext mc) {
    super(LogicalPlanType.UPDATE);
    this.tableName = tableName;
    this.columnName = columnNmae;
    this.attrValue = attrValue;
    this.whereCondition = new ArrayList<>();
    if (mc != null && mc.condition() != null) {
      whereCondition.add(mc.condition().expression(0).getText());
      whereCondition.add(mc.condition().comparator().getText());
      whereCondition.add(mc.condition().expression(1).getText());
    } else {
      this.whereCondition = null;
    }

    rowsHasUpdate = new ArrayList<>();
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
    Column columnToUpdate = null;

    int primaryKeyIndex = -1;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(columnName)) {
        columnToUpdate = columns.get(i);
        columnIdxToUpdate = i;
        if (columnToUpdate.isPrimary()) {
          primaryKeyIndex = i;
        }
        break;
      }
    }
    if (columnToUpdate == null) {
      throw new UpdateErrorException(errColumnName);
    }

    Comparable valueToUpdate = null;
    if (attrValue == null && columnToUpdate.isNotNull()) {
      throw new InsertErrorException(
          "Exception:  insert operation error ( "
              + columnToUpdate.getColumnName()
              + " cannot be null)");
    } else {
      switch (columnToUpdate.getColumnType()) {
        case INT:
          try {
            valueToUpdate = Integer.parseInt(attrValue);
          } catch (NumberFormatException e) {
            throw new InsertErrorException(errColumnType);
          }
          break;
        case LONG:
          try {
            valueToUpdate = Long.parseLong(attrValue);
          } catch (NumberFormatException e) {
            throw new InsertErrorException(errColumnType);
          }
          break;
        case DOUBLE:
          try {
            valueToUpdate = Double.parseDouble(attrValue);
          } catch (NumberFormatException e) {
            throw new InsertErrorException(errColumnType);
          }
          break;
        case FLOAT:
          try {
            valueToUpdate = Float.parseFloat(attrValue);
          } catch (NumberFormatException e) {
            throw new InsertErrorException(errColumnType);
          }
          break;
        case STRING:
          if (attrValue.length() > columnToUpdate.getMaxLength()) {
            throw new InsertErrorException(errStringLength);
          }
          valueToUpdate = attrValue;
      }
    }

    Iterator<Row> rowIterator = table.iterator();
    if (whereCondition == null) {
      if (primaryKeyIndex != -1) {
        if (table.index.size() > 1) {
          throw new UpdateErrorException(duplicateKey);
        } else {
          if (rowIterator.hasNext()) {
            Row oldRow = rowIterator.next();
            Row newRow = getNewRow(oldRow, valueToUpdate);
            if (newRow != null) {
              table.update(oldRow, newRow);
              rowsHasUpdate.add(new Pair<>(oldRow, newRow));
            }
          }
        }
      } else {
        while (rowIterator.hasNext()) {
          Row oldRow = rowIterator.next();
          Row newRow = getNewRow(oldRow, valueToUpdate);
          if (newRow == null) {
            continue;
          }
          table.update(oldRow, newRow);
          rowsHasUpdate.add(new Pair<>(oldRow, newRow));
        }
      }
    } else {
      ArrayList<String> columnNames = new ArrayList<>();
      for (Column column : columns) {
        columnNames.add(column.getColumnName());
      }
      QueryTable qTable = new QueryTable(table, columnNames, whereCondition);
      qTable.execute();
      Entry entryToUpdate = new Entry(valueToUpdate);
      List<Row> rows = qTable.resultRows;
      if (primaryKeyIndex != -1) {
        for (Row row : rows) {
          Row newRow = getNewRow(row, valueToUpdate);
          if (table.index.contains(entryToUpdate)) {
            undo();
            throw new UpdateErrorException(duplicateKey);
          } else {
            table.update(row, newRow);
            rowsHasUpdate.add(new Pair<>(row, newRow));
          }
        }
      } else {
        for (Row row : rows) {
          Row newRow = getNewRow(row, valueToUpdate);
          table.update(row, newRow);
          rowsHasUpdate.add(new Pair<>(row, newRow));
        }
      }
    }
  }

  @Override
  public void undo() {
    for (int i = rowsHasUpdate.size() - 1; i >= 0; i--) {
      table.update(rowsHasUpdate.get(i).right, rowsHasUpdate.get(i).right);
    }
  }

  private Row getNewRow(Row oldRow, Comparable valueToUpdate) {
    ArrayList<Entry> entries = new ArrayList<>();
    ArrayList<Entry> old_entries = oldRow.getEntries();
    for (Entry e : old_entries) {
      entries.add(new Entry(e.value));
    }
    Entry tmp = new Entry(valueToUpdate);
    Entry old = old_entries.get(columnIdxToUpdate);
    if (old.value != null && old.compareTo(tmp) == 0) {
      return null;
    } else {
      entries.set(columnIdxToUpdate, new Entry(valueToUpdate));
    }
    return new Row(entries);
  }

  public String getTableName() {
    return tableName;
  }
}
