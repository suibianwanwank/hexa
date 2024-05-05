package program.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class PushDownHintRel extends SingleRel {

    private final SourceScanExecutionPlan tableScan;
    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input   Input relational expression
     */
    public PushDownHintRel(SourceScanExecutionPlan tableScan, RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
        this.tableScan = tableScan;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (getInputs().equals(inputs)
                && traitSet == getTraitSet()) {
            return this;
        }
        return new PushDownHintRel(tableScan, getCluster(), traitSet, inputs.get(0));
    }

    public SourceScanExecutionPlan getTableScan() {
        return tableScan;
    }
}
