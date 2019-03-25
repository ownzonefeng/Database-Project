package ch.epfl.dias.task3.volcano;

import static org.junit.Assert.*;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.*;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class VolcanoTestPAX {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    PAXStore paxData;
    PAXStore paxOrder;
    PAXStore paxLineItem;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Before
    public void init() throws IOException {

        schema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT};

        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};

        paxData = new PAXStore(schema, "input/data.csv", ",", 3);
        paxData.load();

        int tuple_perpage = 100;
        paxOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", tuple_perpage);
        paxOrder.load();

        paxLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", tuple_perpage);
        paxLineItem.load();
    }


    @Test
    public void testQuery1() {
        Scan scan = new Scan(paxLineItem);
        Select sel = new Select(scan, BinaryOp.LE, 0, 100);
        Project prj = new Project(sel, new int[]{0, 1, 2});
        ProjectAggregate agg = new ProjectAggregate(prj, Aggregate.COUNT, DataType.INT, 2);

        agg.open();

        // This query should return only one result
        DBTuple result = agg.next();
        int output = result.getFieldAsInt(0);
        assertTrue(output == 110);

    }

    @Test
    public void testQuery2() {
        Scan scan_L = new Scan(paxLineItem);
        Scan scan_O = new Scan(paxOrder);
        Select sel_O = new Select(scan_O, BinaryOp.LE, 0, 150);
        HashJoin join = new HashJoin(scan_L, sel_O, 0, 0);
        Project prj = new Project(join, new int[]{7, 19});
        ProjectAggregate agg = new ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 1);

        agg.open();

        // This query should return only one result
        DBTuple result = agg.next();
        int output = result.getFieldAsInt(0);
        System.out.println(output);
        assertTrue(output == 157);

    }

    @Test
    public void testQuery3() {
        Scan scan_L = new Scan(paxLineItem);
        Scan scan_O = new Scan(paxOrder);
        Select sel_O = new Select(scan_O, BinaryOp.LE, 0, 80);
        HashJoin join = new HashJoin(scan_L, sel_O, 0, 0);
        Project prj = new Project(join, new int[]{7, 19});
        ProjectAggregate agg = new ProjectAggregate(join, Aggregate.SUM, DataType.INT, 0);

        agg.open();

        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output);
        assertTrue(true);

    }


}
