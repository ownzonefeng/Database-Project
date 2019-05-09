package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	// Add required structures
	private ColumnarOperator class_ColOp;
	private int[] class_columns;

	public Project(ColumnarOperator child, int[] columns) {
		// Implement
		class_ColOp = child;
		class_columns = columns;
	}

	public DBColumn[] execute() {
		// Implement
		DBColumn[] child_block = class_ColOp.execute();
		boolean lateMaterialization = child_block[0].lateMaterialization;
		DBColumn[] return_block = new DBColumn[class_columns.length];
		if (lateMaterialization) {
			for (int i = 0; i < class_columns.length; i++) {
				DBColumn current_col = child_block[class_columns[i]];
				Object[] new_fields = new Object[current_col.tid.size()];
				for (int j = 0; j < new_fields.length; j++) new_fields[j] = current_col.fields[current_col.tid.get(j)];
				child_block[i].fields = new_fields;
				return_block[i] = child_block[i];
			}
		} else {
			for (int i = 0; i < class_columns.length; i++) return_block[i] = child_block[class_columns[i]];
		}

		return return_block;
	}
}
