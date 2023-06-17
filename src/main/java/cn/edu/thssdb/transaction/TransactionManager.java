package cn.edu.thssdb.transaction;

import cn.edu.thssdb.exception.DatabaseInUseException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionManager {
  private Manager manager;
  private LinkedList<LogicalPlan> plans = new LinkedList<>(); // 操作列表，可能需要回滚
  private HashMap<String, Integer> savepoints; // 检查点
  private LinkedList<ReentrantReadWriteLock.ReadLock> readLockList;
  private LinkedList<ReentrantReadWriteLock.WriteLock> writeLockList;
  private long sessionId;
  private boolean underTransaction = false; // 是否在transaction过程

  public TransactionManager(long sessionId) {
    this.sessionId = sessionId;
    this.manager = Manager.getInstance();
    this.savepoints = new HashMap<>();
    this.readLockList = new LinkedList<>();
    this.writeLockList = new LinkedList<>();
  }

  private Database getDatabase() {
    return manager.getCurrentDatabase(sessionId);
  }

  public TransactionStatus exec(LogicalPlan plan) {
    if (plan instanceof SelectPlan || plan instanceof ShowTablePlan) return readTransaction(plan);
    else if (plan instanceof UpdatePlan || plan instanceof DeletePlan || plan instanceof InsertPlan)
      return writeTransaction(plan);
    else if (plan instanceof CommitPlan) return commitTransaction();
    else if (plan instanceof RollbackPlan)
      return rollbackTransaction(((RollbackPlan) plan).savepointName);
    else if (plan instanceof SavepointPlan)
      return savepointTransaction(((SavepointPlan) plan).savepointName);
    else if (plan instanceof BeginTransactionPlan) return beginTransaction();
    else if (plan instanceof CheckpointPlan) return checkpointTransaction();
    else return endTransaction(plan);
  }

  private TransactionStatus endTransaction(LogicalPlan plan) {
    Database database = getDatabase();
    if (underTransaction) {
      commitTransaction();
    }
    try {
      if (plan instanceof DropDatabasePlan) {
        for (Table table : database.getTables()) {
          if (table.lock.isWriteLocked()) throw new DatabaseInUseException(database.name);
        }
      }
      plan.exec();
      underTransaction = false;
    } catch (Exception e) {
      return new TransactionStatus(false, e.getMessage());
    }
    return new TransactionStatus(true, "");
  }

  private TransactionStatus beginTransaction() {
    Database database = getDatabase();
    if (database == null) throw new DatabaseNotExistException();
    if (underTransaction) return new TransactionStatus(false, "Exception: Transaction ongoing!");
    else {
      underTransaction = true;
      return new TransactionStatus(true, "");
    }
  }

  private TransactionStatus checkpointTransaction() {
    Database database = getDatabase();
    if (underTransaction) {
      commitTransaction();
    }
    if (database == null) throw new DatabaseNotExistException();
    database.persist();
    return new TransactionStatus(true, "");
  }

  private TransactionStatus readTransaction(LogicalPlan plan) {
    if (!Manager.ISOLATION) {
      ArrayList<String> tableNames = getTableName(plan);
      if (tableNames != null)
        for (String tableName : tableNames) {
          this.getTransactionReadLock(tableName);
        }
      try {
        plan.exec();
        underTransaction = true;
      } catch (Exception e) {
        return new TransactionStatus(false, e.getMessage());
      }
      if (tableNames != null)
        for (String tableName : tableNames) {
          this.releaseTransactionReadLock(tableName);
        }
      return new TransactionStatus(true, plan.getMsg());
    }
    if (Manager.ISOLATION) {
      ArrayList<String> tableNames = getTableName(plan);
      if (tableNames != null)
        for (String tableName : tableNames) {
          this.getTransactionReadLock(tableName);
        }
      try {
        plan.exec();
        underTransaction = true;
      } catch (Exception e) {
        return new TransactionStatus(false, e.getMessage());
      }
      return new TransactionStatus(true, plan.getMsg());
    }
    return new TransactionStatus(false, "Exception: Unknown isolation level!");
  }

  private TransactionStatus writeTransaction(LogicalPlan plan) {
    ArrayList<String> tableNames = getTableName(plan);
    if (tableNames != null)
      for (String tableName : tableNames) {
        this.getTransactionWriteLock(tableName);
      }
    try {
      plan.exec();
      plans.add(plan);
      underTransaction = true;
    } catch (Exception e) {
      return new TransactionStatus(false, e.getMessage());
    }
    return new TransactionStatus(true, "Success");
  }

  private TransactionStatus commitTransaction() {
    Database database = getDatabase();
    if (database == null) throw new DatabaseNotExistException();
    this.releaseTransactionReadWriteLock();
    while (!plans.isEmpty()) {
      LogicalPlan plan = plans.getFirst();
      // TODO: log
      plans.removeFirst();
    }
    underTransaction = false;
    return new TransactionStatus(true, "Success");
  }

  private TransactionStatus savepointTransaction(String name) {
    Database database = getDatabase();
    if (database == null) throw new DatabaseNotExistException();
    if (!underTransaction)
      return new TransactionStatus(false, "Exception: No transaction ongoing!");
    if (name == null) return new TransactionStatus(false, "Exception: No savepoint given.");
    savepoints.put(name, plans.size());
    return new TransactionStatus(true, "Success");
  }

  private TransactionStatus rollbackTransaction(String name) {
    Database database = getDatabase();
    if (database == null) throw new DatabaseNotExistException();
    int index = 0;
    if (name != null) {
      Integer tmp = savepoints.get(name);
      if (tmp == null) {
        return new TransactionStatus(false, "Savepoint不存在");
      }
      index = tmp;
    }
    try {
      for (int i = plans.size(); i > index; i--) {
        LogicalPlan plan = plans.removeLast();
        if (plan instanceof SelectPlan || plan instanceof ShowTablePlan) {
          if (Manager.ISOLATION) {
            ArrayList<String> tableNames = getTableName(plan);
            if (tableNames != null)
              for (String tableName : tableNames) {
                this.releaseTransactionReadLock(tableName);
              }
          }
        } else if (plan instanceof UpdatePlan
            || plan instanceof DeletePlan
            || plan instanceof InsertPlan) {
          ArrayList<String> tableNames = getTableName(plan);
          if (tableNames != null)
            for (String tableName : tableNames) {
              this.releaseTransactionWriteLock(tableName);
            }
        }
        // TODO：不是每个plan都有undo
        plan.undo();
      }
      if (index == 0) underTransaction = false;
    } catch (Exception e) {
      return new TransactionStatus(false, e.getMessage());
    }
    return new TransactionStatus(true, "Success");
  }

  private ArrayList<String> getTableName(LogicalPlan plan) {
    ArrayList<String> tableNames = new ArrayList<>();
    if (plan instanceof CreateTablePlan) tableNames.add(((CreateTablePlan) plan).getTableName());
    if (plan instanceof DropTablePlan) tableNames.add(((DropTablePlan) plan).getTableName());
    if (plan instanceof InsertPlan) tableNames.add(((InsertPlan) plan).getTableName());
    if (plan instanceof DeletePlan) tableNames.add(((DeletePlan) plan).getTableName());
    if (plan instanceof UpdatePlan) tableNames.add(((UpdatePlan) plan).getTableName());
    if (plan instanceof SelectPlan) tableNames.addAll(((SelectPlan) plan).getTableName());
    if (plan instanceof ShowTablePlan) tableNames.add(((ShowTablePlan) plan).getTableName());
    return tableNames;
  }

  private boolean getTransactionReadLock(String tableName) {
    Database database = getDatabase();
    Table table = database.get(tableName);
    if (table == null) return false;
    ReentrantReadWriteLock.ReadLock readLock = table.lock.readLock();
    readLock.lock();
    readLockList.add(readLock);
    return true;
  }

  private boolean getTransactionWriteLock(String tableName) {
    Database database = getDatabase();
    Table table = database.get(tableName);
    if (table == null) return false;
    ReentrantReadWriteLock.WriteLock writeLock = table.lock.writeLock();
    if (!writeLock.tryLock()) {
      while (true) {
        if (!this.releaseTransactionReadLock(tableName)) break;
      }
      writeLock.lock();
    }
    writeLockList.add(writeLock);
    return true;
  }

  private boolean releaseTransactionReadLock(String tableName) {
    Database database = getDatabase();
    Table table = database.get(tableName);
    if (table == null) return false;
    ReentrantReadWriteLock.ReadLock readLock = table.lock.readLock();
    if (readLockList.remove(readLock)) {
      readLock.unlock();
      return true;
    }
    return false;
  }

  private boolean releaseTransactionWriteLock(String tableName) {
    Database database = getDatabase();
    Table table = database.get(tableName);
    if (table == null) return false;
    ReentrantReadWriteLock.WriteLock writeLock = table.lock.writeLock();
    if (writeLockList.remove(writeLock)) {
      writeLock.unlock();
      return true;
    }
    return false;
  }

  private void releaseTransactionReadWriteLock() {
    while (!writeLockList.isEmpty()) {
      writeLockList.remove().unlock();
    }
    while (!readLockList.isEmpty()) {
      readLockList.remove().unlock();
    }
  }

  public boolean isUnderTransaction() {
    return underTransaction;
  }
}
