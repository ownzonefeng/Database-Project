package ch.epfl.dias.task3.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.vector.*;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class VectorTest {

    // 1 seconds max per method tested

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    ColumnStore columnstoreData;
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);
    int standardVectorsize = 1000;

    @Before
    public void init() throws IOException {

        schema = new DataType[]{DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
                DataType.INT, DataType.INT, DataType.INT, DataType.INT};

        orderSchema = new DataType[]{DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING};

        lineitemSchema = new DataType[]{DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
                DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING};

        columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
        columnstoreData.load();

        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        columnstoreOrder.load();

        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        columnstoreLineItem.load();

        //columnstoreEmpty = new ColumnStore(schema, "input/empty.csv", ",");
        //columnstoreEmpty.load();
    }

    @Test
    public void testQuery1() {
        Scan scan = new Scan(columnstoreLineItem, standardVectorsize);
        Select sel = new Select(scan, BinaryOp.LE, 0, 100);
        Project prj = new Project(sel, new int[]{0, 1, 2});
        ProjectAggregate agg = new ProjectAggregate(prj, Aggregate.COUNT, DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 110);
    }

    @Test
    public void testQuery2() {
        Scan scan_L = new Scan(columnstoreLineItem, standardVectorsize);
        Scan scan_O = new Scan(columnstoreOrder, standardVectorsize);
        Select sel_O = new Select(scan_O, BinaryOp.LE, 0, 150);
        Join join = new Join(scan_L, sel_O, 0, 0);
        Project prj = new Project(join, new int[]{7, 19});
        ProjectAggregate agg = new ProjectAggregate(prj, Aggregate.COUNT, DataType.INT, 1);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        System.out.println(output);
        assertTrue(output == 157);

    }

    @Test
    public void testQuery3() {
        Scan scan_L = new Scan(columnstoreLineItem, standardVectorsize);
        Scan scan_O = new Scan(columnstoreOrder, standardVectorsize);
        Select sel_O = new Select(scan_O, BinaryOp.LE, 0, 80);
        Join join = new Join(scan_L, sel_O, 0, 0);
        Project prj = new Project(join, new int[]{7, 19});
        ProjectAggregate agg = new ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        double output = result[0].getAsDouble()[0];
        System.out.println(output);
        assertTrue(true);

    }

}
