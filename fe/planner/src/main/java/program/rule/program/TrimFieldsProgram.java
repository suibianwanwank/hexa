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
package program.rule.program;

import context.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import program.program.RuleOptimizeProgram;

public class TrimFieldsProgram implements RuleOptimizeProgram {


    @Override
    public RelNode optimize(RelNode root, QueryContext optimizeContext) {
        RelBuilder relBuilder =
                RelFactories.LOGICAL_BUILDER.create(root.getCluster(), null);
        return new RelFieldTrimmer(null, relBuilder).trim(root);
    }
}
