package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class RollbackPlan extends LogicalPlan {
  public String savepointName;

  public RollbackPlan(String savepointName) {
    super(LogicalPlanType.ROLLBACK);
    this.savepointName = savepointName;
  }

  @Override
  public void exec() {
    // do nothing. transaction manager works.
  }
}
