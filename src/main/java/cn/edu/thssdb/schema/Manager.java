package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.utils.Global;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private HashMap<Long, Database> currentDatabase;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static boolean ISOLATION = true;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<>();
    currentDatabase = new HashMap<>();
    File dir = new File(Global.DATA_ROOT_DIR);
    if (!dir.exists()) {
      dir.mkdirs();
    }
  }

  private boolean databaseExists(String name) {
    return databases.containsKey(name);
  }

  public void createDatabaseIfNotExists(String name, long sessionId) {
    //    if (!databaseExists(name)) {
    //      databases.put(name, new Database(name));
    //    } else {
    //      throw new DuplicateDatabaseException();
    //    }
    try {
      lock.writeLock().lock();
      if (!databaseExists(name)) databases.put(name, new Database(name));
      else throw new DuplicateDatabaseException(name);
      if (!currentDatabase.containsKey(sessionId)) {
        try {
          lock.readLock().lock();
          if (!databaseExists(name)) throw new DatabaseNotExistException(name);
          currentDatabase.put(sessionId, databases.get(name));
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

  public void switchDatabase(String name, long sessionId) {
    try {
      lock.readLock().lock();
      if (!databaseExists(name)) throw new DatabaseNotExistException(name);
      currentDatabase.put(sessionId, databases.get(name));
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

  public Database getCurrentDatabase(long sessionId) {
    return currentDatabase.get(sessionId);
  }

  public void deleteSession(long sessionId) {
    currentDatabase.remove(sessionId);
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
