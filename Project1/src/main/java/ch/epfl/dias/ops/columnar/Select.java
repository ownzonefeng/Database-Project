package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements ColumnarOperator {

	// Add required structures
	private ColumnarOperator class_ColOp;
	private BinaryOp class_op;
	private int class_fieldNo;
	private int class_value;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// Implement
		class_ColOp = child;
		class_op = op;
		class_fieldNo = fieldNo;
		class_value = value;
	}

	@Override
	public DBColumn[] execute() {
		// Implement
		DBColumn[] child_block = class_ColOp.execute();
		DBColumn sel_Column = child_block[class_fieldNo];
		ArrayList<Integer> tids = sel_Column.tid;
		ArrayList<Integer> new_tids = new ArrayList<>();
		int i;
		switch (class_op) {
			case EQ:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] == class_value) new_tids.add(current_tid);
				}
				break;
			case GE:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] >= class_value) new_tids.add(current_tid);
				}
				break;
			case GT:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] > class_value) new_tids.add(current_tid);
				}
				break;
			case LE:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] <= class_value) new_tids.add(current_tid);
				}
				break;
			case LT:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] < class_value) new_tids.add(current_tid);
				}
				break;
			case NE:
				for (i = 0; i < tids.size(); i++) {
					int current_tid = tids.get(i);
					if (sel_Column.getAsDouble()[current_tid] != class_value) new_tids.add(current_tid);
				}
				break;
			default:
				return null;
		}
		child_block[class_fieldNo] = new DBColumn(sel_Column.fields, sel_Column.types, new_tids, sel_Column.lateMaterialization);
		for (i = 0; i < child_block.length; i++) child_block[i].tid = new_tids;

		if (!sel_Column.lateMaterialization) {

			for (i = 0; i < child_block.length; i++) {
				Object[] new_fields = new Object[new_tids.size()];
				ArrayList<Integer> renew_tids = new ArrayList<>();
				for (int j = 0; j < new_tids.size(); j++) {
					new_fields[j] = child_block[i].fields[new_tids.get(j)];
					renew_tids.add(j);
				}
				child_block[i].fields = new_fields;
				child_block[i].tid = renew_tids;
			}
		}
		return child_block;
	}
}
