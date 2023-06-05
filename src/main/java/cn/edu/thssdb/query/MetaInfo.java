package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.List;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;

  public MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
  }

  int columnFind(String name) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getColumnName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  public int columnSize() {
    return columns.size();
  }

  public ColumnType getType(int index) {
    return columns.get(index).getColumnType();
  }

  public String toString() {
    String result = "";
    for (Column column : columns) {
      result += column.toString() + "\n";
    }
    return result;
  }
}
