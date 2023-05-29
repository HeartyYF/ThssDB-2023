package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  public String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    File dir = new File(this.getDatabaseTableFolderPath());
    if (!dir.exists()) dir.mkdirs();
    recover();
  }

  public void persist() {
    // TODO: write data to file
  }

  private boolean tableExists(String name) {
    return tables.containsKey(name);
  }

  public void create(String name, Column[] columns) {
    try {
      lock.writeLock().lock();
      if (tableExists(name)) throw new DuplicateTableException();
      Table table = new Table(this.name, name, columns);
      this.tables.put(name, table);
      this.persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Table get(String tableName) {
    try {
      lock.readLock().lock();
      if (!this.tables.containsKey(tableName)) throw new TableNotExistException();
      return this.tables.get(tableName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void dropDatabase() {
    try {
      lock.writeLock().lock();
      for (Table table : this.tables.values()) {
        //        File file = new File(table.getTableMetaPath());
        //        if (file.isFile()&&!file.delete())
        //          throw new FileIOException(this.name + " _meta when drop the database");
        table.drop();
      }
      this.tables.clear();
      this.tables = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void dropTable(String tableName) {
    try {
      lock.writeLock().lock();
      if (!this.tables.containsKey(tableName)) throw new TableNotExistException(tableName);
      Table table = this.tables.get(tableName);
      //      String filename = table.getTableMetaPath();
      //      File file = new File(filename);
      //      if (file.isFile() && !file.delete())
      //        throw new FileIOException(tableName + " _meta  when drop a table in database");
      table.drop();
      this.tables.remove(tableName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String select(QueryTable[] queryTables) {
    // TODO? may need to transfer from SelectPlan to here
    // currently it is not used
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO: read data from file
  }

  public void quit() {
    // TODO
  }

  public String getDatabasePath() {
    return Global.DATA_ROOT_DIR + File.separator + this.name;
  }

  public String getDatabaseTableFolderPath() {
    return this.getDatabasePath() + File.separator + "tables";
  }

  public Table[] getTables() {
    return this.tables.values().toArray(new Table[tables.size()]);
  }
}
