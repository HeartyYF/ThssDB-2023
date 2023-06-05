package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  private QueryTable[] queryTables;

  public QueryResult(QueryTable[] queryTable) {
    queryTables = queryTable;
    for (QueryTable table : queryTable) {
      table.execute();
    }
  }

  public List<List<String>> getRowList() {
    List<List<String>> result = new LinkedList<>();
    for (QueryTable table : queryTables) {
      for (Row row : table.resultRows) {
        result.add(row.toStringList());
      }
    }
    return result;
  }

  public String getResult() {
    String result = "";
    for (QueryTable table : queryTables) {
      for (Row row : table.resultRows) {
        result += row.toString() + "\n";
      }
    }
    return result;
  }

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    // don't know what this function is for
    return null;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    // don't know what this function is for
    return null;
  }
}
