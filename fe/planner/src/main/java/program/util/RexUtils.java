package program.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Comparator;
import java.util.List;

public class RexUtils {
    private RexUtils() {

    }

    public static boolean containIdentity(List<? extends RexNode> exps, RelDataType rowType, RelDataType childRowType, Comparator<String> nameComparator)
    {
        List<RelDataTypeField> fields = rowType.getFieldList();
        List<RelDataTypeField> childFields = childRowType.getFieldList();
        int fieldCount = childFields.size();
        if (exps.size() != fieldCount) {
            return false;
        }
        for (int i = 0; i < exps.size(); i++) {
            RexNode exp = exps.get(i);
            if (!(exp instanceof RexInputRef)) {
                return false;
            }
            RexInputRef var = (RexInputRef) exp;
            if (var.getIndex() != i) {
                return false;
            }
            if (0 != nameComparator.compare(fields.get(i).getName(), childFields.get(i).getName())) {
                return false;
            }
            if (!fields.get(i).getType().equals(childFields.get(i).getType())) {
                return false;
            }
        }
        return true;
    }
}
