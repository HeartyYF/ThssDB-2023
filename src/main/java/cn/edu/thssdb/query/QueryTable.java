package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.AmbiguousColumnException;
import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.TableNotMatchException;
import cn.edu.thssdb.schema.*;

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
  ArrayList<Integer> tableColumnOrder = new ArrayList<>();
  ArrayList<Integer> joinIndexes = new ArrayList<>();
  public List<Row> resultRows;
  public Iterator<Row> rowIterator;

  public QueryTable(
      long sessionId,
      ArrayList<String> tableNames,
      ArrayList<String> columnNames,
      ArrayList<String> condition,
      ArrayList<String> columnTableNames,
      ArrayList<String> joinCondition) {
    for (String tableName : tableNames) {
      tables.add(Manager.getInstance().getCurrentDatabase(sessionId).get(tableName));
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

  private void addIndexOrder(int index, int tableIndex) {
    if (indexContains(index, tableIndex)) {
      return;
    }
    columnIndexes.add(index);
    tableColumnOrder.add(tableIndex);
  }

  private void addIndexOrderAll(int tableIndex, int size) {
    for (int i = 0; i < size; i++) {
      addIndexOrder(i, tableIndex);
    }
  }

  private boolean indexContains(int index, int tableIndex) {
    if (columnIndexes.contains(index)) {
      if (tableColumnOrder.get(columnIndexes.indexOf(index)).equals(tableIndex)) {
        return true;
      }
    }
    return false;
  }

  private void checkJoinValid(ArrayList<String> joinCondition, ArrayList<String> condition) {
    if (joinCondition.size() == 4) {
      if (joinCondition.get(0).equals(tables.get(0).tableName)) {
        if (joinCondition.get(0).equals(tables.get(1).tableName)) {
          throw new TableNotMatchException(joinCondition.get(0));
        } else {
          String tableName = joinCondition.get(0);
          joinCondition.set(0, joinCondition.get(2));
          joinCondition.set(2, tableName);
          String columnName = joinCondition.get(1);
          joinCondition.set(1, joinCondition.get(3));
          joinCondition.set(3, columnName);
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
              addIndexOrderAll(2, tables.get(1).getMetaInfo().columnSize());
            }
          } else {
            addIndexOrderAll(1, tables.get(0).getMetaInfo().columnSize());
          }
        } else {
          addIndexOrderAll(1, tables.get(0).getMetaInfo().columnSize());
          addIndexOrderAll(2, tables.get(1).getMetaInfo().columnSize());
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
              addIndexOrder(index, 2);
            }
          } else {
            int index = tables.get(0).getMetaInfo().columnFind(columnName);
            if (index == -1) {
              throw new ColumnNotExistException(columnName);
            }
            addIndexOrder(index, 1);
          }
        } else {
          int index = tables.get(0).getMetaInfo().columnFind(columnName);
          if (index == -1) {
            index = tables.get(1).getMetaInfo().columnFind(columnName);
            if (index == -1) {
              throw new ColumnNotExistException(columnName);
            }
            addIndexOrder(index, 2);
          } else {
            int index2 = tables.get(1).getMetaInfo().columnFind(columnName);
            if (index2 == -1) {
              addIndexOrder(index, 1);
            } else {
              throw new AmbiguousColumnException(columnName);
            }
          }
        }
      }
    }
    if (indexContains(joinIndexes.get(0), 1) && indexContains(joinIndexes.get(1), 2)) {
      for (int i = 0; i < columnIndexes.size(); i++) {
        if (columnIndexes.get(i).equals(joinIndexes.get(0)) && tableColumnOrder.get(i).equals(1)) {
          columnIndexes.remove(i);
          tableColumnOrder.remove(i);
          break;
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
          table = tables.get(1);
          comparedColumnIndex = metaInfo.columnFind(condition.get(2));
          comparedValue = getTypedValue(condition.get(0), comparedColumnIndex);
          comparedColumnIndex = columnJoinFind(metaInfo.columnFind(condition.get(2)), 2);
          comparator = comparatorSwap(condition.get(1));
        }
      } else {
        table = tables.get(0);
        comparedColumnIndex = metaInfo.columnFind(condition.get(0));
        comparedValue = getTypedValue(condition.get(2), comparedColumnIndex);
        comparedColumnIndex = columnJoinFind(metaInfo.columnFind(condition.get(0)), 1);
        comparator = condition.get(1);
      }
    }
    // System.out.println(columnIndexes);
    // System.out.println(tableColumnOrder);
  }

  private int columnJoinFind(int index, int tableIndex) {
    int i = 0;
    for (; i < columnIndexes.size(); i++) {
      if (columnIndexes.get(i) == index && tableColumnOrder.get(i) == tableIndex) {
        break;
      }
    }
    return i;
  }

  private void handleJoinTable() {
    resultRows = new ArrayList<>();
    int index1 = joinIndexes.get(0);
    int index2 = joinIndexes.get(1);
    // System.out.println(comparedColumnIndex);

    for (Row row1 : tables.get(0)) {
      for (Row row2 : tables.get(1)) {
        if (row1.getEntry(index1).equals(row2.getEntry(index2))) {
          Row row = new Row(row1, row2, columnIndexes, tableColumnOrder);
          if (conditionCheck(row)) {
            resultRows.add(row);
          }
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

  public List<Column> getColumns() {
    List<Column> res = new ArrayList<>();
    if (tables.size() >= 2) {
      for (int i = 0; i < columnIndexes.size(); i++) {
        res.add(tables.get(tableColumnOrder.get(i)).getColumn(columnIndexes.get(i)));
      }
    } else {
      for (int index : columnIndexes) {
        res.add(table.getColumn(index));
      }
    }
    return res;
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
