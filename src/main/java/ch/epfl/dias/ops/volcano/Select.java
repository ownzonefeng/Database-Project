package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// Add required structures
	private VolcanoOperator class_VolOp;
	private BinaryOp class_op;
	private int class_fieldNo;
	private int class_value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// Implement
		this.class_VolOp = child;
		this.class_op = op;
		this.class_fieldNo = fieldNo;
		this.class_value = value;
	}

	@Override
	public void open() {
		// Implement
		class_VolOp.open();
	}

	@Override
	public DBTuple next() {
		// Implement
		DBTuple current_tuple = class_VolOp.next();
		if (current_tuple.eof) return current_tuple;
		int fieldValue = current_tuple.getFieldAsInt(this.class_fieldNo);
		switch (this.class_op) {
			case LT:
				if (fieldValue < this.class_value) return current_tuple;
				break;
			case LE:
				if (fieldValue <= this.class_value) return current_tuple;
				break;
			case EQ:
				if (fieldValue == this.class_value) return current_tuple;
				break;
			case NE:
				if (fieldValue != this.class_value) return current_tuple;
				break;
			case GE:
				if (fieldValue >= this.class_value) return current_tuple;
				break;
			case GT:
				if (fieldValue > this.class_value) return current_tuple;
				break;
			default:
				return this.next();
		}
		return this.next();
	}

	@Override
	public void close() {
		// Implement
		this.class_VolOp.close();
	}
}
