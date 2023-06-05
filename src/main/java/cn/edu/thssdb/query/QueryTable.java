package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AmbiguousColumnException;
import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.TableNotMatchException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {
  ArrayList<Table> tables = new ArrayList<>();
  Table table;
  ArrayList<String> columnNames;
  ArrayList<String> columnTableNames;
  Entry comparedValue;
  String comparator;
  int comparedColumnIndex;
  ArrayList<Integer> columnIndexes = new ArrayList<>();
  ArrayList<Integer> columnIndexes2 = new ArrayList<>();
  ArrayList<Integer> joinIndexes = new ArrayList<>();
  public List<Row> resultRows;
  public Iterator<Row> rowIterator;

  public QueryTable(
      ArrayList<String> tableNames,
      ArrayList<String> columnNames,
      ArrayList<String> condition,
      ArrayList<String> columnTableNames,
      ArrayList<String> joinCondition) {
    for (String tableName : tableNames) {
      tables.add(Manager.getInstance().getCurrentDatabase().get(tableName));
    }
    this.columnNames = columnNames;
    this.columnTableNames = columnTableNames;
    if (joinCondition.size() == 4 && tableNames.size() == 2) {
      checkJoinValid(joinCondition, condition);
    } else {
      this.table = tables.get(0);
      checkValid(condition);
    }
  }

  public QueryTable(Table table, ArrayList<String> columnNames, ArrayList<String> condition) {
    this.table = table;
    this.columnNames = columnNames;
    checkValid(condition);
  }

  private void checkJoinValid(ArrayList<String> joinCondition, ArrayList<String> condition) {
    if (joinCondition.size() == 4) {
      if (joinCondition.get(0) != tables.get(0).tableName) {
        if (joinCondition.get(0) != tables.get(1).tableName) {
          throw new TableNotMatchException(joinCondition.get(0));
        } else {
          Table temp = tables.get(0);
          tables.set(0, tables.get(1));
          tables.set(1, temp);
        }
      }
      MetaInfo metaInfo1 = tables.get(0).getMetaInfo();
      int index1 = metaInfo1.columnFind(joinCondition.get(1));
      MetaInfo metaInfo2 = tables.get(1).getMetaInfo();
      int index2 = metaInfo2.columnFind(joinCondition.get(3));
      if (index1 == -1) {
        throw new ColumnNotExistException(joinCondition.get(1));
      }
      if (index2 == -1) {
        throw new ColumnNotExistException(joinCondition.get(3));
      }
      joinIndexes.add(index1);
      joinIndexes.add(index2);
    }
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnNames.get(i).equals("*")) {
        if (columnTableNames.get(i) != null) {
          String tableName = columnTableNames.get(i);
          if (!tableName.equals(tables.get(0).tableName)) {
            if (!tableName.equals(tables.get(1).tableName)) {
              throw new TableNotMatchException(columnTableNames.get(i));
            } else {
              columnIndexes2 = null;
            }
          } else {
            columnIndexes = null;
          }
        } else {
          columnIndexes = null;
          columnIndexes2 = null;
        }
      } else {
        String columnName = columnNames.get(i);
        if (columnTableNames.get(i) != null) {
          String tableName = columnTableNames.get(i);
          if (!tableName.equals(tables.get(0).tableName)) {
            if (!tableName.equals(tables.get(1).tableName)) {
              throw new TableNotMatchException(columnTableNames.get(i));
            } else {
              int index = tables.get(1).getMetaInfo().columnFind(columnName);
              if (index == -1) {
                throw new ColumnNotExistException(columnName);
              }
              if (columnIndexes2 == null) {
                continue;
              }
              columnIndexes2.add(index);
            }
          } else {
            int index = tables.get(0).getMetaInfo().columnFind(columnName);
            if (index == -1) {
              throw new ColumnNotExistException(columnName);
            }
            if (columnIndexes == null) {
              continue;
            }
            columnIndexes.add(index);
          }
        } else {
          int index = tables.get(0).getMetaInfo().columnFind(columnName);
          if (index == -1) {
            index = tables.get(1).getMetaInfo().columnFind(columnName);
            if (index == -1) {
              throw new ColumnNotExistException(columnName);
            }
            if (columnIndexes2 == null) {
              continue;
            }
            columnIndexes2.add(index);
          } else {
            int index2 = tables.get(1).getMetaInfo().columnFind(columnName);
            if (index2 == -1) {
              if (columnIndexes == null) {
                continue;
              }
              columnIndexes.add(index);
            } else {
              throw new AmbiguousColumnException(columnName);
            }
          }
        }
      }
    }
    if (condition != null && condition.size() == 3) {
      MetaInfo metaInfo;
      if (condition.get(0).contains(".")) {
        String tableName = condition.get(0).split("\\.")[0];
        if (!tableName.equals(tables.get(0).tableName)) {
          if (!tableName.equals(tables.get(1).tableName)) {
            throw new TableNotMatchException(tableName);
          } else {
            metaInfo = tables.get(1).getMetaInfo();
          }
        } else {
          metaInfo = tables.get(0).getMetaInfo();
        }
        condition.set(0, condition.get(0).split("\\.")[1]);
      } else if (condition.get(2).contains(".")) {
        String tableName = condition.get(2).split("\\.")[0];
        if (!tableName.equals(tables.get(0).tableName)) {
          if (!tableName.equals(tables.get(1).tableName)) {
            throw new TableNotMatchException(tableName);
          } else {
            metaInfo = tables.get(1).getMetaInfo();
          }
        } else {
          metaInfo = tables.get(0).getMetaInfo();
        }
        condition.set(2, condition.get(2).split("\\.")[1]);
      } else {
        throw new ColumnNotExistException(condition.get(0));
      }
      if (metaInfo.columnFind(condition.get(0)) == -1) {
        if (metaInfo.columnFind(condition.get(2)) == -1) {
          throw new ColumnNotExistException(condition.get(0));
        } else {
          comparedColumnIndex = metaInfo.columnFind(condition.get(2));
          comparator = comparatorSwap(condition.get(1));
          comparedValue = getTypedValue(condition.get(0), comparedColumnIndex);
        }
      } else {
        comparedColumnIndex = metaInfo.columnFind(condition.get(0));
        comparator = condition.get(1);
        comparedValue = getTypedValue(condition.get(2), comparedColumnIndex);
      }
    }
  }

  private void handleJoinTable() {
    resultRows = new ArrayList<>();
    int index1 = joinIndexes.get(0);
    int index2 = joinIndexes.get(1);
    for (Row row1 : tables.get(0)) {
      for (Row row2 : tables.get(1)) {
        if (row1.getEntry(index1).equals(row2.getEntry(index2))) {
          Row row = new Row(new Row(row1, columnIndexes), new Row(row2, columnIndexes2, index2));
          resultRows.add(row);
        }
      }
    }
    rowIterator = resultRows.iterator();
  }

  private void checkValid(ArrayList<String> condition) {
    MetaInfo metaInfo = table.getMetaInfo();
    for (String columnName : columnNames) {
      if (columnName.equals("*")) {
        columnIndexes = null;
        break;
      }
      int index = metaInfo.columnFind(columnName);
      if (index == -1) {
        throw new ColumnNotExistException(columnName);
      }
      columnIndexes.add(index);
    }
    if (condition != null && condition.size() == 3) {
      if (metaInfo.columnFind(condition.get(0)) == -1) {
        if (metaInfo.columnFind(condition.get(2)) == -1) {
          throw new ColumnNotExistException(condition.get(0));
        } else {
          comparedColumnIndex = metaInfo.columnFind(condition.get(2));
          comparator = comparatorSwap(condition.get(1));
          comparedValue = getTypedValue(condition.get(0), comparedColumnIndex);
        }
      } else {
        comparedColumnIndex = metaInfo.columnFind(condition.get(0));
        comparator = condition.get(1);
        comparedValue = getTypedValue(condition.get(2), comparedColumnIndex);
      }
    }
    // System.out.println("value: " + comparedValue);
    // System.out.println("comparator: " + comparator);
  }

  public void execute() {
    if (tables.size() == 2) {
      handleJoinTable();
      return;
    }
    Iterator<Row> tableIterator = table.iterator();
    resultRows = new ArrayList<>();
    while (tableIterator.hasNext()) {
      Row row = tableIterator.next();
      if (comparedValue == null) {
        resultRows.add(row);
      } else {
        if (conditionCheck(row)) {
          if (columnIndexes == null) {
            resultRows.add(row);
          } else {
            resultRows.add(new Row(row, columnIndexes));
          }
        }
      }
    }
    rowIterator = resultRows.iterator();
  }

  private Entry getTypedValue(String value, int columnIndex) {
    switch (table.getMetaInfo().getType(columnIndex)) {
      case INT:
        return new Entry(Integer.parseInt(value));
      case LONG:
        return new Entry(Long.parseLong(value));
      case FLOAT:
        return new Entry(Float.parseFloat(value));
      case DOUBLE:
        return new Entry(Double.parseDouble(value));
      case STRING:
        return new Entry(value);
      default:
        return new Entry(value);
    }
  }

  private boolean conditionCheck(Row row) {
    if (comparedValue == null) {
      return true;
    }
    switch (comparator) {
      case "=":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) == 0;
      case "<":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) < 0;
      case ">":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) > 0;
      case "<=":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) <= 0;
      case ">=":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) >= 0;
      case "<>":
        return row.getEntries().get(comparedColumnIndex).compareTo(comparedValue) != 0;
      default:
        return false;
    }
  }

  private String comparatorSwap(String ori) {
    switch (ori) {
      case "<":
        return ">";
      case ">":
        return "<";
      case "<=":
        return ">=";
      case ">=":
        return "<=";
      default:
        return ori;
    }
  }

  @Override
  public boolean hasNext() {
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    return rowIterator.next();
  }
}
