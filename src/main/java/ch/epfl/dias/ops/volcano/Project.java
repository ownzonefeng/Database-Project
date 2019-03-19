package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// Add required
	private VolcanoOperator class_VolOp;
	private int[] class_fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		// Implement
		this.class_VolOp = child;
		this.class_fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// Implement
		this.class_VolOp.open();
	}

	@Override
	public DBTuple next() {
		// Implement
		DBTuple current_tuple = this.class_VolOp.next();
        if (current_tuple.eof) return current_tuple;
		Object[] new_fields = new Object[this.class_fieldNo.length];
		DataType[] new_schema = new DataType[this.class_fieldNo.length];
		for (int i = 0; i < this.class_fieldNo.length; i++) {
			switch (current_tuple.types[this.class_fieldNo[i]]) {
				case INT:
					new_fields[i] = current_tuple.getFieldAsInt(this.class_fieldNo[i]);
					new_schema[i] = DataType.INT;
					break;
				case DOUBLE:
					new_fields[i] = current_tuple.getFieldAsDouble(this.class_fieldNo[i]);
					new_schema[i] = DataType.DOUBLE;
					break;
				case STRING:
					new_fields[i] = current_tuple.getFieldAsString(this.class_fieldNo[i]);
					new_schema[i] = DataType.STRING;
					break;
				case BOOLEAN:
					new_fields[i] = current_tuple.getFieldAsBoolean(this.class_fieldNo[i]);
					new_schema[i] = DataType.BOOLEAN;
					break;
			}
		}
		DBTuple return_tuple = new DBTuple(new_fields, new_schema);
		return return_tuple;
	}

	@Override
	public void close() {
		// Implement
		this.class_VolOp.close();
	}
}
