package cn.edu.thssdb.plan.impl;

public class SwitchDatabasePlan {
  private String databaseName;

  public SwitchDatabasePlan(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
