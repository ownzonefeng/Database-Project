package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.lang.Math;

public class ProjectAggregate implements VolcanoOperator {

	// Add required structures
	private VolcanoOperator class_VolOp;
	private Aggregate class_agg;
	private DataType class_dt;
	private int class_fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// Implement
		this.class_VolOp = child;
		this.class_agg = agg;
		this.class_dt = dt;
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
		Object[] new_fields = new Object[1];
		DataType[] new_types = new DataType[1];
		new_types[0] = this.class_dt;
		int count = 0;
		double value = 0;
		double agg_value = 0;
		if (this.class_agg == Aggregate.COUNT) {
			while (!current_tuple.eof) {
				count++;
				current_tuple = this.class_VolOp.next();
			}
			value = count * count;
		} else {
			switch (this.class_agg) {
				case AVG:
					while (!current_tuple.eof) {
						count++;
						value = value + current_tuple.getFieldAsDouble(this.class_fieldNo);
						current_tuple = this.class_VolOp.next();
					}
					break;
				case MAX:
					value = Double.MIN_VALUE;
					while (!current_tuple.eof) {
						count = 1;
						value = Math.max(value, current_tuple.getFieldAsDouble(this.class_fieldNo));
						current_tuple = this.class_VolOp.next();
					}
					break;
				case MIN:
					value = Double.MAX_VALUE;
					while (!current_tuple.eof) {
						count = 1;
						value = Math.min(value, current_tuple.getFieldAsDouble(this.class_fieldNo));
						current_tuple = this.class_VolOp.next();
					}
					break;
				case SUM:
					while (!current_tuple.eof) {
						count = 1;
						value = value + current_tuple.getFieldAsDouble(this.class_fieldNo);
						current_tuple = this.class_VolOp.next();
					}
					break;
			}
		}
		agg_value = value / count;
		new_fields[0] = agg_value;
		DBTuple return_tuple = new DBTuple(new_fields, new_types);
		return return_tuple;
	}

	@Override
	public void close() {
		// Implement
		this.class_VolOp.close();
	}

}
