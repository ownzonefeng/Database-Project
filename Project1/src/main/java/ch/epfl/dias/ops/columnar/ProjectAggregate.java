package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class ProjectAggregate implements ColumnarOperator {

	// Add required structures
	private ColumnarOperator class_ColOp;
	private Aggregate class_agg;
	private DataType class_dt;
	private int class_fieldNo;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// Implement
		class_ColOp = child;
		class_agg = agg;
		class_dt = dt;
		class_fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		// Implement
		DBColumn op_col = class_ColOp.execute()[class_fieldNo];
		ArrayList<Integer> tids = op_col.tid;
		int count = 0;
		double value = 0;
		int i;
		switch (class_agg) {
			case SUM:
				value = 0;
				for (i = 0; i < tids.size(); i++) value = value + op_col.getAsDouble()[tids.get(i)];
				break;
			case MIN:
				value = Double.MAX_VALUE;
				for (i = 0; i < tids.size(); i++) value = min(value, op_col.getAsDouble()[tids.get(i)]);
				break;
			case MAX:
				value = Double.MIN_VALUE;
				for (i = 0; i < tids.size(); i++) value = max(value, op_col.getAsDouble()[tids.get(i)]);
				break;
			case AVG:
				value = 0;
				for (i = 0; i < tids.size(); i++) value = value + op_col.getAsDouble()[tids.get(i)];
				value = value / tids.size();
				break;
			case COUNT:
				count = tids.size();
				break;
		}
        DBColumn return_col = new DBColumn(new Object[]{value}, class_dt);
        if (class_agg == Aggregate.COUNT) return_col = new DBColumn(new Object[]{count}, class_dt);
		return new DBColumn[]{return_col};
	}
}
