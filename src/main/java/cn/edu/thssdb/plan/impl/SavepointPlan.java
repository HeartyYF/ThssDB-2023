package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class SavepointPlan extends LogicalPlan {
  public String savepointName;

  public SavepointPlan(String savepointName) {
    super(LogicalPlanType.BEGIN_TRANSACTION);
    this.savepointName = savepointName;
  }

  @Override
  public void exec() {
    // do nothing. transaction manager works.
  }
}
