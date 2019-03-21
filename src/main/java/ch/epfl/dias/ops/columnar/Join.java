package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.awt.*;
import java.util.*;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	// Add required structures
	private ColumnarOperator class_left;
	private ColumnarOperator class_right;
	private int class_leftNo;
	private int class_rightNo;
	private Map<Object, ArrayList<Integer>> hashtable = new HashMap<>();

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// Implement
		class_left = leftChild;
		class_right = rightChild;
		class_leftNo = leftFieldNo;
		class_rightNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		// Implement
		DBColumn[] left_block = class_left.execute();
		DBColumn[] right_block = class_right.execute();
		ArrayList<Integer> left_tids = left_block[class_leftNo].tid;
		Object[] left_field = left_block[class_leftNo].fields;

		for (Integer left_tid : left_tids) {
			if (hashtable.containsKey(left_field[left_tid])) {
				ArrayList<Integer> existed_arr_list = hashtable.get(left_field[left_tid]);
				existed_arr_list.add(left_tid);
				hashtable.put(left_field[left_tid], existed_arr_list);
			} else {
				ArrayList<Integer> current_arr_list = new ArrayList<>();
				current_arr_list.add(left_tid);
				hashtable.put(left_field[left_tid], current_arr_list);
			}
		}

		ArrayList<Integer> right_tids = right_block[class_rightNo].tid;
		Object[] right_field = right_block[class_rightNo].fields;
		ArrayList<Integer> new_left_tids = new ArrayList<>();
		ArrayList<Integer> new_right_tids = new ArrayList<>();

		for (Integer right_tid : right_tids) {
			Object element_right = right_field[right_tid];
			if (hashtable.containsKey(element_right)) {
				ArrayList<Integer> temp_right = new ArrayList<Integer>(Collections.nCopies(hashtable.get(element_right).size(), right_tid));
				new_right_tids.addAll(temp_right);
				new_left_tids.addAll(hashtable.get(element_right));
			}
		}
		if (new_left_tids.size() != new_right_tids.size()) throw new RuntimeException("Cannot join");
		for (DBColumn dbColumn : left_block) dbColumn.tid = new_left_tids;
		for (DBColumn dbColumn : right_block) dbColumn.tid = new_right_tids;
		DBColumn[] return_cols = new DBColumn[left_block.length + right_block.length];
		System.arraycopy(left_block, 0, return_cols, 0, left_block.length);
		System.arraycopy(right_block, 0, return_cols, left_block.length, right_block.length);

		if (left_block[class_leftNo].lateMaterialization) return return_cols;
		else {
			for (DBColumn current_col : return_cols) {
				Object[] new_field = new Object[current_col.tid.size()];
				ArrayList<Integer> renew_tids = new ArrayList<>();
				for (int i = 0; i < current_col.tid.size(); i++) {
					new_field[i] = current_col.fields[current_col.tid.get(i)];
					renew_tids.add(i);
				}
				current_col.fields = new_field;
				current_col.tid = renew_tids;
			}
			return return_cols;
		}
	}
}
