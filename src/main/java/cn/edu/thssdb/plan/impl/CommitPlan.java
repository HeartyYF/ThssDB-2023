package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class CommitPlan extends LogicalPlan {
  public CommitPlan() {
    super(LogicalPlanType.COMMIT);
  }

  @Override
  public void exec() {
    // do nothing. transaction manager works.
  }
}
