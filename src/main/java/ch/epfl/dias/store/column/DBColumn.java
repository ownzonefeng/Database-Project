package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// Implement
	public int[] fields;
	public DataType[] types;

	public DBColumn(int[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
	}
	
	public int[] getAsInteger() {
		// Implement
		return this.fields;
	}
}
