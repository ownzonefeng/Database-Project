package ch.epfl.dias;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test_read {
    public static void main(String[] args){
        ArrayList<Integer> test = new ArrayList<>();
        test.add(5);
        test.add(1);
        test.add(3);
        test.add(7);
        System.out.println(test.toString());
        ArrayList<Integer> test2 = new ArrayList<>();
        test2.add(22);
        test2.add(33);
        test2.add(55);
        test.addAll(test2);
        System.out.println(test.toString());
        List<Integer> list = new ArrayList<Integer>(Collections.nCopies(10, 7));
        System.out.println(list.toString());



        }
    }

