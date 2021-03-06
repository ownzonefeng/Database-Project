package ch.epfl.dias;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import java.io.IOException;


public class Main {

	public static void main(String[] args) throws IOException {

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		RowStore rowstore = new RowStore(orderSchema, "input/orders_test.csv", "\\|");
		rowstore.load();
		PAXStore paxstore = new PAXStore(orderSchema, "input/orders_test.csv", "\\|", 3);
		paxstore.load();

		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
		DBTuple currentTuple = scan.next();
		while (!currentTuple.eof) {
			System.out.println(currentTuple.getFieldAsInt(1));
			currentTuple = scan.next();
		}

		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();
		/*
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		DBColumn[] result = agg.execute();
		int output = result[0].getAsInteger()[0];
		System.out.println(output);
		*/
	}
}
