package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.stream.DoubleStream;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class ProjectAggregate implements VectorOperator {

	// Add required structures
	private VectorOperator class_vec_op;
	private Aggregate class_agg;
	private DataType class_dt;
	private int class_fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// Implement
		class_vec_op = child;
		class_agg = agg;
		class_dt = dt;
		class_fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// Implement
		class_vec_op.open();
	}

	@Override
	public DBColumn[] next() {
		// Implement
		DBColumn[] cols_to_select;
		while (true) {
			cols_to_select = class_vec_op.next();
			if (cols_to_select != null) break;
		}
		if (cols_to_select[0].eof) throw new RuntimeException("Empty agg");
		DataType new_dt = cols_to_select[class_fieldNo].types;
		int count = 0;
		double value = 0;
		switch (class_agg) {
			case COUNT:
				while (!cols_to_select[0].eof) {
					count = count + cols_to_select[class_fieldNo].fields.length;
					while (true) {
						cols_to_select = class_vec_op.next();
						if (cols_to_select != null) break;
					}
				}
				break;
			case AVG:
				while (!cols_to_select[0].eof) {
					count = count + cols_to_select[class_fieldNo].fields.length;
					value = value + DoubleStream.of(cols_to_select[class_fieldNo].getAsDouble()).sum();
					while (true) {
						cols_to_select = class_vec_op.next();
						if (cols_to_select != null) break;
					}
				}
				value = value / count;
				break;
			case MAX:
				value = Double.MIN_VALUE;
				while (!cols_to_select[0].eof) {
					double current_max = DoubleStream.of(cols_to_select[class_fieldNo].getAsDouble()).max().orElse(Double.MIN_VALUE);
					value = max(value, current_max);
					while (true) {
						cols_to_select = class_vec_op.next();
						if (cols_to_select != null) break;
					}
				}
				break;
			case MIN:
				value = Double.MAX_VALUE;
				while (!cols_to_select[0].eof) {
					double current_min = DoubleStream.of(cols_to_select[class_fieldNo].getAsDouble()).min().orElse(Double.MAX_VALUE);
					value = min(value, current_min);
					while (true) {
						cols_to_select = class_vec_op.next();
						if (cols_to_select != null) break;
					}
				}
				break;
			case SUM:
				while (!cols_to_select[0].eof) {
					value = value + DoubleStream.of(cols_to_select[class_fieldNo].getAsDouble()).sum();
					while (true) {
						cols_to_select = class_vec_op.next();
						if (cols_to_select != null) break;
					}
				}
				break;
		}
		ArrayList<Integer> new_tid = new ArrayList<>();
		new_tid.add(0);
		Object[] new_fields = new Object[1];
		if (class_agg == Aggregate.COUNT) {
			new_fields[0] = count;
		} else {
			new_fields[0] = value;
		}
		DBColumn return_col = new DBColumn(new_fields, new_dt, new_tid, false);
		return new DBColumn[]{return_col};
	}

	@Override
	public void close() {
		// Implement
		class_vec_op.close();
	}

}
