package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	// Add required structures
    public DataType[] class_schema;
	private String class_filename;
	private String class_delimiter;
    public Boolean class_lateMaterialization;
    public DBColumn[] data;


	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		// Implement
		this.class_schema = schema;
		this.class_filename = filename;
		this.class_delimiter = delimiter;
		this.class_lateMaterialization = lateMaterialization;
	}

	@Override
	public void load() throws IOException {
		// Implement

		Path path = Paths.get(this.class_filename);
		List<String> contents = Files.readAllLines(path);
		int size = contents.size();
		int length = contents.get(0).split(this.class_delimiter).length;
		String[][] content_array = new String[size][length];
		for(int i = 0; i < size; i ++)
		{
			String[] row;
			row = contents.get(i).split(this.class_delimiter);
			System.arraycopy(row, 0, content_array[i], 0, length);
		}

		this.data = new DBColumn[length];
		for(int j = 0; j < length; j ++)
		{
			Object[] col = new Object[size];
            ArrayList<Integer> tid = new ArrayList<>();
			for(int i = 0; i < size; i ++)
			{
				col[i] = content_array[i][j];
                tid.add(i);
			}
            this.data[j] = new DBColumn(col, this.class_schema[j], tid, this.class_lateMaterialization);
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// Implement
		int size = columnsToGet.length;
		DBColumn[] get_columns = new DBColumn[size];
		for(int i = 0; i < size; i ++)
		{
			get_columns[i] = this.data[columnsToGet[i]];
		}
		return get_columns;
	}
}
