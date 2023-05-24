package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryTable implements Iterator<Row> {
  Table table;
  ArrayList<String> columnNames;
  Entry comparedValue;
  String comparator;
  int comparedColumnIndex;
  ArrayList<Integer> columnIndexes = new ArrayList<>();
  List<Row> resultRows;
  Iterator<Row> rowIterator;

  public QueryTable(Table table, ArrayList<String> columnNames, ArrayList<String> condition) {
    this.table = table;
    this.columnNames = columnNames;
    checkValid(condition);
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
