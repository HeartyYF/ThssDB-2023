package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // TODO: write data to file
  }

  private boolean tableExists(String name) {
    return tables.containsKey(name);
  }

  public void create(String name, Column[] columns) {
    if (!tableExists(name)) {
      tables.put(name, new Table(this.name, name, columns));
    } else {
      throw new DuplicateTableException();
    }
  }

  public void drop() {
    if (tableExists(name)) {
      tables.remove(name);
    } else {
      throw new TableNotExistException();
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO: read data from file
  }

  public void quit() {
    // TODO
  }
}
