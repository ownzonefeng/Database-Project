package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

import java.util.ArrayList;
import java.util.Arrays;

public class DBColumn {

	// Implement
	public Object[] fields;
	public DataType types;
	public ArrayList<Integer> tid;
	public boolean lateMaterialization;

	public DBColumn(Object[] fields, DataType types, ArrayList<Integer> tuple_ids, boolean late_Materialization) {
		this.fields = fields;
		this.types = types;
		this.tid = tuple_ids;
		this.lateMaterialization = late_Materialization;
	}

	public DBColumn(Object[] fields, DataType types) {
		this.fields = fields;
		this.types = types;
	}

	public int[] getAsInteger() {
		// Implement
		int[] fields_int;
		fields_int = Arrays.stream(this.fields).mapToInt(value -> Integer.parseInt(value.toString())).toArray();
		return fields_int;
	}

	public double[] getAsDouble() {
		// Implement
		double[] fields_double;
		fields_double = Arrays.stream(this.fields).mapToDouble(value -> Double.parseDouble(value.toString())).toArray();
		return fields_double;
	}

	public Object getAsObject(int fieldNo)
	{
		return this.fields[fieldNo];
	}
}
