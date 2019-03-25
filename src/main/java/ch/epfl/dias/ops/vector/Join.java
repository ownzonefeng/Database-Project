package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Join implements VectorOperator {

	// Add required structures
	private VectorOperator class_left;
	private VectorOperator class_right;
	private int class_leftNo;
	private int class_rightNo;
	private Map<Object, ArrayList<Integer>> hash_table = new HashMap<>();
	private ArrayList<DBColumn[]> left_vectors = new ArrayList<>();
	private int vector_length;
	private int left_length;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// Implement
		class_left = leftChild;
		class_right = rightChild;
		class_leftNo = leftFieldNo;
		class_rightNo = rightFieldNo;
	}

	@Override
	public void open() {
		// Implement
		class_left.open();
		class_right.open();
        DBColumn[] col_left;
        while (true) {
            col_left = class_left.next();
            if (col_left != null) break;
        }
        if (col_left[0].eof) throw new RuntimeException("Null Left Object");
		left_length = col_left.length;
		vector_length = col_left[0].fields.length;
		int i = 0;
        while (!col_left[0].eof) {
			left_vectors.add(col_left);
			for (Object value : col_left[class_leftNo].fields) {
				if (!hash_table.containsKey(value)) {
					ArrayList<Integer> tids = new ArrayList<>();
					tids.add(i);
					i++;
					hash_table.put(value, tids);
				} else {
					ArrayList<Integer> old_tids = hash_table.get(value);
					old_tids.add(i);
					i++;
					hash_table.put(value, old_tids);

				}
			}
			col_left = class_left.next();
		}
	}

	@Override
	public DBColumn[] next() {
		// Implement
        DBColumn[] col_right;
        while (true) {
            col_right = class_right.next();
            if (col_right != null) break;
        }
        if (col_right[0].eof) return col_right;
		int right_length = col_right.length;
		ArrayList<Integer> new_left_tids = new ArrayList<>();
		ArrayList<Integer> new_right_tids = new ArrayList<>();
		int right_tid = 0;
		for (Object value : col_right[class_rightNo].fields) {
			if (hash_table.containsKey(value)) {
				ArrayList<Integer> temp_right = new ArrayList<Integer>(Collections.nCopies(hash_table.get(value).size(), right_tid));
				new_left_tids.addAll(hash_table.get(value));
				new_right_tids.addAll(temp_right);
			}
			right_tid++;
		}
		if (new_left_tids.size() != new_right_tids.size()) throw new RuntimeException("Cannot join");
		int col_length = new_left_tids.size();
		DBColumn[] return_cols = new DBColumn[left_length + right_length];
		ArrayList<Integer> renew_tids = new ArrayList<>();
		for (int id = 0; id < col_length; id++) renew_tids.add(id);

		for (int i = 0; i < return_cols.length; i++) {
			Object[] current_col = new Object[col_length];
			if (i < left_length) {
				DataType dt = left_vectors.get(0)[i].types;
				for (int j = 0; j < col_length; j++) {
					int current_tid = new_left_tids.get(j);
					int vec_id = current_tid / vector_length;
					int off_set = current_tid % vector_length;
					current_col[j] = left_vectors.get(vec_id)[i].fields[off_set];
				}
				return_cols[i] = new DBColumn(current_col, dt, renew_tids, false);
			} else {
				DataType dt = col_right[i - left_length].types;
				for (int j = 0; j < col_length; j++) {
					int current_tid = new_right_tids.get(j);
					current_col[j] = col_right[i - left_length].fields[current_tid];
				}
				return_cols[i] = new DBColumn(current_col, dt, renew_tids, false);
			}

		}

		return return_cols;
	}

	@Override
	public void close() {
		// Implement
		class_left.close();
		class_right.close();
	}
}
