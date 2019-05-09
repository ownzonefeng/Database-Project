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
    private Map<Object, ArrayList<Integer>> hash_table = new HashMap<>();
    private List<DBTuple> left_tuples = new ArrayList<>();
    private DBTuple lag_right_tuple;
    private boolean lag = false;
    private int lag_idx = 0;

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
            DBTuple current_tuple;
            while (true) {
                current_tuple = class_left.next();
                if (current_tuple != null) break;
            }
            if (current_tuple.eof) break;
            left_tuples.add(current_tuple);
            if (hash_table.containsKey(current_tuple.fields[class_leftNo])) {
                ArrayList<Integer> old_idx = hash_table.get(current_tuple.fields[class_leftNo]);
                old_idx.add(i);
                hash_table.put(current_tuple.fields[class_rightNo], old_idx);
            } else {
                ArrayList<Integer> new_idx_list = new ArrayList<>();
                new_idx_list.add(i);
                hash_table.put(current_tuple.fields[class_leftNo], new_idx_list);
            }
        }
    }


    @Override
    public DBTuple next() {
        // Implement
        DBTuple tuple_right;
        if (!lag) {
            while (true) {
                tuple_right = class_right.next();
                if (tuple_right != null) break;
            }
            this.lag_right_tuple = tuple_right;
        } else tuple_right = this.lag_right_tuple;

        if (tuple_right.eof) return tuple_right;

        if (hash_table.containsKey(tuple_right.fields[class_rightNo])) {
            ArrayList<Integer> join_idx = hash_table.get(tuple_right.fields[class_rightNo]);
            int idx;
            if (join_idx.size() == lag_idx + 1) {
                lag = false;
                idx = join_idx.get(lag_idx);
                lag_idx = 0;
            } else {
                lag = true;
                idx = join_idx.get(lag_idx);
                lag_idx++;
            }
            DBTuple tuple_left = this.left_tuples.get(idx);
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
        // Implement
        class_left.close();
        class_right.close();
        hash_table = null;
        lag = false;
        lag_idx = 0;
        left_tuples = new ArrayList<>();
        hash_table = new HashMap<>();
    }
}
