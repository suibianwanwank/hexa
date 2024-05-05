/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package program.rule.shuttle;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

public class SearchRemoveShuttle
        extends RelShuttleImpl {
    private final RexBuilder rexBuilder;

    public SearchRemoveShuttle(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        LogicalFilter newFilter = (LogicalFilter) super.visit(filter);
        RexNode condition = newFilter.getCondition();
        if (condition instanceof RexCall) {
            RexShuttle rexShuttle = RexUtil.searchShuttle(rexBuilder, null, -1);
            RexNode newCondition = condition.accept(rexShuttle);
            return newFilter.copy(newFilter.getTraitSet(), newFilter.getInput(), newCondition);
        }
        return newFilter;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        LogicalJoin newJoin = (LogicalJoin) super.visit(join);
        RexNode condition = newJoin.getCondition();
        if (condition instanceof RexCall) {
            RexShuttle rexShuttle = RexUtil.searchShuttle(rexBuilder, null, -1);
            RexNode newCondition = condition.accept(rexShuttle);
            return newJoin.copy(newJoin.getTraitSet(), newCondition, newJoin.getLeft(), newJoin.getRight(), newJoin.getJoinType(), newJoin.isSemiJoinDone());
        }
        return newJoin;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        LogicalProject newProject = (LogicalProject) super.visit(project);
        List<RexNode> projects = newProject.getProjects();
        List<RexNode> newProjects = new ArrayList<>();
        for (RexNode node : projects) {
            if (node instanceof RexCall) {
                RexShuttle rexShuttle = RexUtil.searchShuttle(rexBuilder, null, -1);
                RexNode newExpr = node.accept(rexShuttle);
                newProjects.add(newExpr);
                continue;
            }
            newProjects.add(node);
        }
        return newProject.copy(newProject.getTraitSet(), newProject.getInput(), ImmutableList.copyOf(newProjects), newProject.getRowType());
    }
}
