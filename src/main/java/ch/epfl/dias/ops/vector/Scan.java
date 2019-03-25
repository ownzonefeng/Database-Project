package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

import static java.lang.Math.min;

public class Scan implements VectorOperator {

	// Add required structures
	private Store class_store;
	private int class_vec_size;
	private int current_tid;
	private DBColumn[] class_data;
	private int col_length;
	private int tuple_length;

	public Scan(Store store, int vectorsize) {
		// Implement
		class_store = store;
		class_vec_size = vectorsize;
	}
	
	@Override
	public void open() {
		// Implement
		current_tid = 0;
		class_data = class_store.getColumns(new int[]{});
		col_length = class_data[0].tid.size();
		tuple_length = class_data.length;
	}

	@Override
	public DBColumn[] next() {
		// Implement
		if (current_tid >= col_length) return new DBColumn[]{new DBColumn()};
		int current_vec_size = min(col_length - current_tid, class_vec_size);
		DBColumn[] return_columns = new DBColumn[tuple_length];
		int i = 0;
		for (DBColumn current_col : class_data) {
			Object[] new_fields = new Object[current_vec_size];
			System.arraycopy(current_col.fields, current_tid, new_fields, 0, current_vec_size);
			ArrayList<Integer> new_tid = current_col.tid;
			new_tid.removeIf(value -> (value >= current_vec_size));
			boolean late_material = false;
			return_columns[i] = new DBColumn(new_fields, current_col.types, new_tid, late_material);
			i = i + 1;
		}
		current_tid = current_tid + class_vec_size;
		return return_columns;
	}

	@Override
	public void close() {
		// Implement
		current_tid = 0;
		class_store = null;
		class_vec_size = 0;
		class_data = null;
		col_length = 0;
		tuple_length = 0;
	}
}
