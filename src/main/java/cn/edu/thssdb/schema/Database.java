package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.plan.LogicalGenerator;

import java.nio.file.Paths;
import java.io.File;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  public String name;
  private HashMap<String, Table> tables;
  private ReentrantReadWriteLock lock;
  private Meta meta;
  private LogManager logger;                      // 日志管理

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    File dir = new File(this.getDatabaseTableFolderPath());
    if (!dir.exists()) dir.mkdirs();
    String meta_name = name + ".meta";
    this.meta = new Meta(this.getDatabaseTableFolderPath(), meta_name);
    String logger_name = name + ".log";
    this.logger = new LogManager(this.getDatabaseTableFolderPath(), logger_name);
    recover();
  }

  public synchronized void persist() {
    ArrayList<String> keys = new ArrayList<>();
    for(String key: tables.keySet())
    {
      tables.get(key).persist();
      keys.add(key);
    }

    this.meta.writeToFile(keys);
    this.logger.eraseFile();

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
      this.meta.deleteFile();
      Paths.get(Global.DATA_ROOT_DIR, name).toFile().delete();
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
    ArrayList<String[]> table_list = this.meta.readFromFile();
    for (String [] table_info: table_list) {
      tables.put(table_info[0], new Table(this.name, table_info[0]));
    }
    logRecover(); //恢复
  }

  public void logRecover() {
    try {
      ArrayList<String> logs = this.logger.readLog();
      for (String log: logs) {
        String [] info = log.split(" ");
        String type = info[0];
        if (type.equals("DELETE")) {
          tables.get(info[1]).delete(info[2]);
        } else if (type.equals("INSERT")) {
          tables.get(info[1]).insert(info[2]);
        } else if (!type.equals("COMMIT")) {
          String [] commands = log.split("\n");
          for (int i = 1; i < commands.length; i++) {
            try {
              LogicalPlan plan = LogicalGenerator.generate(commands[i]);
//            plan.setCurrentUser(null, name);
              plan.exec();
            } catch (Exception e) {

            }
          }
        }
      }
    } catch (Exception e) {
      throw new TableNotExistException();
    }
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
