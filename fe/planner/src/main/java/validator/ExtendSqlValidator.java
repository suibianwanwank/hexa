package validator;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class ExtendSqlValidator extends SqlValidatorImpl {
    private final SqlTypeMappingRule mappingRule;

    protected ExtendSqlValidator(SqlOperatorTable opTab,
                                 SqlValidatorCatalogReader catalogReader,
                                 RelDataTypeFactory typeFactory,
                                 Config config,
                                 SqlTypeMappingRule mappingRule) {
        super(opTab, catalogReader, typeFactory, config);
        this.mappingRule = mappingRule;
    }
}
