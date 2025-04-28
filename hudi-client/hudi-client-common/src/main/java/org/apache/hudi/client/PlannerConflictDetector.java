package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.util.Map;

/**
 * Detects conflicts with a plan that was recently generated when compared to an up to date timeline.
 */
public class PlannerConflictDetector {
  private final long skew;

  public PlannerConflictDetector(long skew) {
    this.skew = skew;
  }

  public boolean hasConflictWithCompactionPlan(HoodieCompactionPlan compactionPlan, long plannerStartTime, HoodieTimeline currentTimeline) {
    return false;
  }

  public boolean hasConflictWithLogCompactionPlan(HoodieCompactionPlan logCompactionPlan, long plannerStartTime, HoodieTimeline currentTimeline) {
    return false;
  }

  public boolean hasConflictWithClusteringPlan(HoodieClusteringPlan clusteringPlan, long plannerStartTime, HoodieTimeline currentTimeline) {
    return false;
  }

  public boolean hasConflictWithCleanerPlan(HoodieCleanerPlan clusteringPlan, long plannerStartTime, HoodieTimeline currentTimeline) {
    return false;
  }

  private Map<String, String> getFileGroupsInCompletedDeltaCommits(HoodieTimeline timeline, long startTime) {
    return null;
  }

  private Map<String, String> getFileGroupsInCompactionPlans(HoodieTimeline timeline, long startTime) {
    return null;
  }

  private Map<String, String> getFileGroupsInClusteringPlans(HoodieTimeline timeline, long startTime) {
    return null;
  }

  private Map<String, String> getFileGroupsInRollbackPlans(HoodieTimeline timeline, long startTime) {
    return null;
  }
}
