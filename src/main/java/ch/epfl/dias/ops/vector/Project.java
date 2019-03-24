package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

    // Add required structures
    private VectorOperator class_vec_op;
    private int[] class_fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
        // Implement
        class_vec_op = child;
        class_fieldNo = fieldNo;
	}

	@Override
	public void open() {
        // Implement
        class_vec_op.open();
	}

	@Override
	public DBColumn[] next() {
        // Implement
        DBColumn[] cols_to_select = class_vec_op.next();
        DBColumn[] return_cols = new DBColumn[class_fieldNo.length];
        for (int i = 0; i < class_fieldNo.length; i++) {
            return_cols[i] = cols_to_select[class_fieldNo[i]];
        }
        return return_cols;
	}

	@Override
	public void close() {
        // Implement
        class_vec_op.close();
	}
}
