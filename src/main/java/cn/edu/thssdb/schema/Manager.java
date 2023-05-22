package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private Database currentDatabase;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<>();
    currentDatabase = null;
    File dir = new File(Global.DATA_ROOT_DIR);
    if (!dir.exists()) {
      dir.mkdirs();
    }
  }

  private boolean databaseExists(String name) {
    return databases.containsKey(name);
  }

  public void createDatabaseIfNotExists(String name) {
    //    if (!databaseExists(name)) {
    //      databases.put(name, new Database(name));
    //    } else {
    //      throw new DuplicateDatabaseException();
    //    }
    try {
      lock.writeLock().lock();
      if (!databaseExists(name)) databases.put(name, new Database(name));
      else throw new DuplicateDatabaseException(name);
      if (currentDatabase == null) {
        try {
          lock.readLock().lock();
          if (!databaseExists(name)) throw new DatabaseNotExistException(name);
          currentDatabase = databases.get(name);
        } finally {
          lock.readLock().unlock();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String name) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(name)) throw new DatabaseNotExistException(name);
      Database database = databases.get(name);
      database.dropDatabase();
      databases.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase(String name) {
    try {
      lock.readLock().lock();
      if (!databaseExists(name)) throw new DatabaseNotExistException(name);
      currentDatabase = databases.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Database get(String databaseName) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(databaseName)) throw new DatabaseNotExistException(databaseName);
      return databases.get(databaseName);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Database getCurrentDatabase() {
    return currentDatabase;
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
