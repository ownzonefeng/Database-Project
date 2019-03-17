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
import java.util.List;

public class Test_read {
    public static void main(String[] args){
        DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
                DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };
        Path path = Paths.get("input/data.csv");
        try{

            List contents = Files.readAllLines(path);
            //Read from the stream
            Object[] data = contents.toArray();
            System.out.println(data[0].toString().split(",").length);




            } catch (IOException e) {
            e.printStackTrace();
        }


        }
    }

