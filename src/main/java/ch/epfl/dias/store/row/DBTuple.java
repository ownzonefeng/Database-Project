package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;

public class DBTuple {
	public Object[] fields;
	public DataType[] types;
	public boolean eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
		this.eof = false;
	}

	public DBTuple() {
		this.eof = true;
	}

	/**
	 * XXX Assuming that the caller has ALREADY checked the datatype, and has
	 * made the right call
	 * 
	 * @param fieldNo
	 *            (starting from 0)
	 * @return cast of field
	 */
	public Integer getFieldAsInt(int fieldNo) { return Integer.parseInt(fields[fieldNo].toString()); } // made some changes

	public Double getFieldAsDouble(int fieldNo) { return Double.parseDouble(fields[fieldNo].toString()); } // made some changes

	public Boolean getFieldAsBoolean(int fieldNo) { return Boolean.parseBoolean(fields[fieldNo].toString()); } // made some changes

	public String getFieldAsString(int fieldNo) {
		return (String) fields[fieldNo];
	}
}
