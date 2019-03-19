package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// Add required structures
	private VolcanoOperator class_left;
	private VolcanoOperator class_right;
	private int class_leftNo;
	private int class_rightNo;
	private Map<Object, Object> hash_table = new HashMap<>();

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// Implement
		this.class_left = leftChild;
		this.class_right = rightChild;
		this.class_leftNo = leftFieldNo;
		this.class_rightNo = rightFieldNo;

	}

	@Override
	public void open() {
		// Implement
		class_left.open();
		class_right.open();
		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			DBTuple current_tuple = class_left.next();
			if (current_tuple.eof) break;
			hash_table.put(current_tuple.fields[class_leftNo], current_tuple);
		}
	}

	@Override
	public DBTuple next() {
		// Implement
		DBTuple tuple_right = class_right.next();
		if (!tuple_right.eof && hash_table.containsKey(tuple_right.fields[class_rightNo])) {
			DBTuple tuple_left = (DBTuple) hash_table.get(tuple_right.fields[class_rightNo]);
			int len_left = tuple_left.fields.length;
			int len_right = tuple_right.fields.length;
			int types_len_left = tuple_left.types.length;
			int types_len_right = tuple_right.types.length;
			Object[] new_fields = new Object[len_left + len_right];
			DataType[] new_types = new DataType[types_len_left + types_len_right];
			System.arraycopy(tuple_left.fields, 0, new_fields, 0, len_left);
			System.arraycopy(tuple_right.fields, 0, new_fields, len_left, len_right);
			System.arraycopy(tuple_left.types, 0, new_types, 0, types_len_left);
			System.arraycopy(tuple_right.types, 0, new_types, types_len_left, types_len_right);
			return new DBTuple(new_fields, new_types);
		}
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
