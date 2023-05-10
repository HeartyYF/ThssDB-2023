package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;

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
  }

  private boolean databaseExists(String name) {
    return databases.containsKey(name);
  }

  public void createDatabaseIfNotExists(String name) {
    if (!databaseExists(name)) {
      databases.put(name, new Database(name));
    } else {
      throw new DuplicateDatabaseException();
    }
  }

  public void dropDatabase(String name) {
    if (databaseExists(name)) {
      databases.remove(name);
    } else {
      throw new DatabaseNotExistException();
    }
  }

  public void switchDatabase(String name) {
    if (databaseExists(name)) {
      currentDatabase = databases.get(name);
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
