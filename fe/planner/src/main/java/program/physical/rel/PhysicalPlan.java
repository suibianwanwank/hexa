package program.physical.rel;

import arrow.datafusion.PhysicalPlanNode;

public interface PhysicalPlan {
    PhysicalPlanNode transformToPP();
}