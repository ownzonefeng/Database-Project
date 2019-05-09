package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements ColumnarOperator {

	// Add required structures
	private ColumnStore class_store;

	public Scan(ColumnStore store) {
		// Implement
		class_store = store;
	}

	@Override
	public DBColumn[] execute() {
		// Implement
		return class_store.data;
	}
}
