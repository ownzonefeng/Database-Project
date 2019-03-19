package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// Add required structures
	private Store class_store;
	private int current_index;

	public Scan(Store store) {
		// Implement
		this.class_store = store;
		open();
	}

	@Override
	public void open() {
		// Implement
		this.current_index = 0;
	}

	@Override
	public DBTuple next() {
		// Implement
		DBTuple return_tuple = class_store.getRow(this.current_index);
		this.current_index++;
		return return_tuple;
	}

	@Override
	public void close() {
		// Implement
		this.class_store = null;
		this.current_index = 0;
	}
}