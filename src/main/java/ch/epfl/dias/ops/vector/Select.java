package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Select implements VectorOperator {

	// Add required structures
	private VectorOperator class_vec_op;
	private BinaryOp class_op;
	private int class_fieldNo;
	private int class_value;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// Implement
		class_vec_op = child;
		class_op = op;
		class_fieldNo = fieldNo;
		class_value = value;
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
		if (cols_to_select[0].eof) return cols_to_select;
		int col_length = cols_to_select[0].tid.size();
		ArrayList<Integer> new_tids = new ArrayList<>();
		switch (class_op) {
			case NE:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] != class_value) new_tids.add(i);
				}
				break;
			case LT:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] < class_value) new_tids.add(i);
				}
				break;
			case LE:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] <= class_value) new_tids.add(i);
				}
				break;
			case GT:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] > class_value) new_tids.add(i);
				}
				break;
			case GE:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] >= class_value) new_tids.add(i);
				}
				break;
			case EQ:
				for (int i = 0; i < col_length; i++) {
					if (cols_to_select[class_fieldNo].getAsDouble()[i] == class_value) new_tids.add(i);
				}
				break;
		}
		if (new_tids.size() == 0) return null;


		DBColumn[] return_columns = new DBColumn[cols_to_select.length];
		for (int i = 0; i < return_columns.length; i++) {
			ArrayList<Integer> renew_tids = new ArrayList<>();
			Object[] new_fields = new Object[new_tids.size()];
			for (int j = 0; j < new_tids.size(); j++) {
				new_fields[j] = cols_to_select[i].fields[new_tids.get(j)];
				renew_tids.add(j);
			}
			return_columns[i] = new DBColumn(new_fields, cols_to_select[i].types, renew_tids, false);
		}
		return return_columns;
	}

	@Override
	public void close() {
		// Implement
		class_vec_op.close();
	}
}
