package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;


public class RowStore extends Store {

	// Add required structures
	private DataType[] class_schema;
	private String class_filename;
	private String class_delimiter;
	private DBTuple[] orders;


	public RowStore(DataType[] schema, String filename, String delimiter) {
		// Implement
		this.class_schema = schema;
		this.class_filename = filename;
		this.class_delimiter = delimiter;
	}

	@Override
	public void load() throws IOException {
		// Implement
		Path path = Paths.get(this.class_filename);
		List contents = Files.readAllLines(path);
		int size = contents.size();
		this.orders = new DBTuple[size];
		Object[] content = contents.toArray();
		for(int i = 0; i < size; i ++)
		{
			orders[i] = new DBTuple(content[i].toString().split(this.class_delimiter), this.class_schema);
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		// Implement
		return this.orders[rownumber];
	}
}
