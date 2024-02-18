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
package convert;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * The Context for expand view, the core is sqlConverter
 */
public class SqlToRelContext implements RelOptTable.ToRelContext {
    private final SqlConverter sqlConverter;

    public SqlToRelContext(SqlConverter sqlConverter) {
        this.sqlConverter = sqlConverter;
    }

    @Override
    public RelOptCluster getCluster() {
        return sqlConverter.getRelOptCluster();
    }

    @Override
    public List<RelHint> getTableHints() {
        return Collections.emptyList();
    }

    public SqlConverter getSqlConverter() {
        return sqlConverter;
    }

    @Override
    public RelRoot expandView(RelDataType relDataType, String s, List<String> list, @Nullable List<String> list1) {
        return null;
    }
}
