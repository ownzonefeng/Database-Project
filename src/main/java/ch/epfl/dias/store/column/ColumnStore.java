package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	// Add required structures
	private DataType[] class_schema;
	private String class_filename;
	private String class_delimiter;
	private Boolean class_lateMaterialization;
	private DBColumn[] data;

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
		List contents = Files.readAllLines(path);
		int size = contents.size();
		Object[] content = contents.toArray();
		int length = content[0].toString().split(this.class_delimiter).length;
		String[][] content_array = new String[size][length];
		for(int i = 0; i < size; i ++)
		{
			String[] row = new String[length];
			row = content[i].toString().split(this.class_delimiter);
			for(int j = 0; j < length; j ++)
			{
				content_array[i][j] = row[j];
			}
		}

		this.data = new DBColumn[length];
		for(int j = 0; j < length; j ++)
		{
			int[] col = new int[size];
			for(int i = 0; i < size; i ++)
			{
				col[i] = Integer.parseInt(content_array[i][j]);
			}
			this.data[j] = new DBColumn(col, this.class_schema);
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
