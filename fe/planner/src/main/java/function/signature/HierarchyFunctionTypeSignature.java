package function.signature;

import function.FunctionBodySignature;
import org.apache.calcite.sql.type.SqlTypeName;
@Deprecated
public class HierarchyFunctionTypeSignature extends FunctionTypeSignature {

    private SqlTypeName sqlTypeName;
    private FunctionTypeSignature hierarchyTypeSignature;


    public static HierarchyFunctionTypeSignature of(SqlTypeName sqlTypeName,
                                                    FunctionTypeSignature hierarchyTypeSignature) {
        return new HierarchyFunctionTypeSignature(sqlTypeName, hierarchyTypeSignature);
    }

    private HierarchyFunctionTypeSignature(SqlTypeName sqlTypeName,
                                           FunctionTypeSignature hierarchyTypeSignature) {
        this.sqlTypeName = sqlTypeName;
        this.hierarchyTypeSignature = hierarchyTypeSignature;
    }

    @Override
    public boolean isSameFunctionType(FunctionTypeSignature functionTypeSignature,
                                      FunctionBodySignature functionBodySignature) {
        if (!(functionTypeSignature instanceof HierarchyFunctionTypeSignature)) {
            return false;
        }
        FunctionTypeSignature hierarchyTypeSignature1 =
                ((HierarchyFunctionTypeSignature) functionTypeSignature).getHierarchyTypeSignature();
        return sqlTypeName.equals(functionTypeSignature.getFunctionType())
                && hierarchyTypeSignature.isSameFunctionType(hierarchyTypeSignature1, functionBodySignature);
    }

    @Override
    public SqlTypeName getFunctionType() {
        return sqlTypeName;
    }

    @Override
    public SqlTypeName getDerivedFunctionType() {
        return hierarchyTypeSignature.getFunctionType();
    }

    public FunctionTypeSignature getHierarchyTypeSignature() {
        return hierarchyTypeSignature;
    }
}
