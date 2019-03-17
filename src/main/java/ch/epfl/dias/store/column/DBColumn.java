package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

import java.util.Arrays;

public class DBColumn {

	// Implement
	public Object[] fields;
	public DataType[] types;

	public DBColumn(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
	}

	public int[] getAsInteger() {
		// Implement
		int[] fields_int;
		fields_int = Arrays.stream(this.fields).mapToInt(value -> Integer.parseInt(value.toString())).toArray();
		return fields_int;
	}

	public Object getAsObject(int fieldNo)
	{
		return this.fields[fieldNo];
	}
}
