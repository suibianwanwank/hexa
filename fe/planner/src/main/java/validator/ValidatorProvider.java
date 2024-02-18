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

package validator;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;

public class ValidatorProvider {

    private static final SqlOperatorTable DEFAULT_OPERATOR_TABLE = SqlStdOperatorTable.instance();

    private ValidatorProvider() {
    }

    public static SqlValidatorWithHints create(Prepare.CatalogReader catalogReader) {
        return new ExtendSqlValidator(DEFAULT_OPERATOR_TABLE, catalogReader, catalogReader.getTypeFactory(),
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true)
                        .withCallRewrite(false)
                        .withTypeCoercionEnabled(true), SqlTypeCoercionRule.instance());
    }
}
