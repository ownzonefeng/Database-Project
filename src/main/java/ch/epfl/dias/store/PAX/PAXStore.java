package ch.epfl.dias.store.PAX;

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
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import static java.lang.Math.min;

public class PAXStore extends Store {

	// Add required structures
	private DataType[] class_schema;
	private String class_filename;
	private String class_delimiter;
	private int tuplesPerPage;
	private DBPAXpage[] orders;
	private int max_row;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// Implement
		this.class_schema = schema;
		this.class_filename = filename;
		this.class_delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException {
		// Implement
		Path path = Paths.get(this.class_filename);
		List<String> contents = Files.readAllLines(path);
		int size = contents.size();
		this.max_row = size;
		int pagesNo = size / this.tuplesPerPage + 1;
		orders = new DBPAXpage[pagesNo];
		for(int i = 0; i < pagesNo; i ++)
		{
			int tuples_in_page;
			tuples_in_page = min(this.tuplesPerPage, size - i * this.tuplesPerPage);
			String[][] single_page = new String[tuples_in_page][];

			for(int j = i * this.tuplesPerPage; j < i * this.tuplesPerPage + tuples_in_page; j ++)
			{
				int current_idx = j - i * this.tuplesPerPage;
				single_page[current_idx] = contents.get(j).split(this.class_delimiter);
			}

			int subpagesNo = single_page[0].length;
			DBColumn[] subpages = new DBColumn[subpagesNo];

			for(int j = 0; j < subpagesNo; j ++)
			{
				String[] col_in_subpage = new String[tuples_in_page];
				for(int k = 0; k < tuples_in_page; k ++)
				{
					col_in_subpage[k] = single_page[k][j];
				}
				DBColumn subpage = new DBColumn(col_in_subpage, new DataType[] {this.class_schema[j]});
				subpages[j] = subpage;
			}

			orders[i] = new DBPAXpage(subpages, this.class_schema);
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		// Implement
		if (rownumber >= this.max_row) return new DBTuple();
		int get_page_no = rownumber / this.tuplesPerPage;
		int get_offset = rownumber % this.tuplesPerPage;
		DBPAXpage page = this.orders[get_page_no];
		return page.get_tuple(get_offset);
	}
}
