package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.Arrays;

public class DBPAXpage {

	// Implement
    private DBColumn[] fields;
    private DataType[] types;
    private int length;

    public DBPAXpage(DBColumn[] fields, DataType[] types)
    {
        this.fields = fields;
        this.types = types;
        this.length = this.fields.length;
    }

    public DBTuple get_tuple(int offset_number)
    {
        Object[] tuple = new Object[this.length];
        for(int i = 0; i < this.length; i ++)
        {
            tuple[i] = fields[i].getAsObject(offset_number);
        }

        return new DBTuple(tuple, this.types);
    }

}
