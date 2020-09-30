package com.taskjdbc.jdbc;

import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class JDBCTest {
	static int queries = 0;
	String url;
	Connection connection;
	Statement readStatement;
	Statement st ;

	JDBCTest() throws SQLException {
		this.url = "jdbc:sqlserver://EN410266\\sqlexpress;user=sa3;password=Password@123;databaseName=BikeStores";
		this.connection = DriverManager.getConnection(url);
		this.readStatement = connection.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		this.st = connection.createStatement();
	}

	public void queryData(String query, JDBCTest test) throws SQLException {
		queries++;
		System.out.println("*********************************************");
		System.out.println("Running query : " + queries);
		System.out.println("*********************************************");
		ResultSet result =  readStatement.executeQuery(query);
		ResultSetMetaData metadata = result.getMetaData();

		int count = 0 ;
		while(result.next()) {
			count++; result.absolute(count);
			if(count == 1) {
				for(int i=1;i<=metadata.getColumnCount();i++) {
					System.out.print(metadata.getColumnLabel(i) + " | ");
					if(i == metadata.getColumnCount() ) {
						System.out.println("\n----------------------------------------------");
					}
				};
			};

			for(int i=1;i<=metadata.getColumnCount();i++) {
				System.out.print(result.getString(i) + " | ");
			};
			System.out.println();
		}
		System.out.println("-----------------------------------------------------\n");
		System.out.println("Total records : " + count);
		System.out.println("\n\n");
	}

	public void updateData(String query, JDBCTest test) throws SQLException {
		st.executeUpdate(query);
	}

	public void deleteData(String query, JDBCTest test) throws  SQLException {
		st.executeUpdate(query);
	}

	public void insertData(String query, JDBCTest test) throws  SQLException {
		st.executeUpdate(query);
	}

	// Rows must be less than 100000
	public void bulkInsert(int rows) throws IOException, SQLException { // example taken from : https://stackoverflow.com/questions/3784197/efficient-way-to-do-batch-inserts-with-jdbc
		List<List<String>> records = new ArrayList<>();
		String csvName = "BulkInsert-"+rows;
		BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\BulkInsert-1000000.csv"));

		String line;
		int counter = 0;
		while ((line = br.readLine()) != null) {
			String[] values = line.split(",");
			records.add(Arrays.asList(values));
			counter++;
			if(counter == rows) { break ;}
		}

		PreparedStatement preparedStatement;

		//Get dynamically columns names from records and formulate insert query statement
		String fields = "INSERT INTO sales.customers(";
		String values = " VALUES(";
		for(int i=1;i<records.get(0).size();i++) {
			if(i<records.get(0).size()-1) {
				fields += records.get(0).get(i) + ",";
				values += "?,";
			}else {
				fields += records.get(0).get(i)+")";
				values += "?);";
			}
		}
		String insertQuery = fields+"\n"+values;

		connection.setAutoCommit(true);
		preparedStatement = connection.prepareStatement(insertQuery);

		for(int i=1 ; i < rows; i++) {
			for(int j=1;j<records.get(i).size();j++) {
				preparedStatement.setString(j, records.get(i).get(j));
			}
			preparedStatement.addBatch();
		}

		long start = System.currentTimeMillis();
		int[] inserted = preparedStatement.executeBatch();
		long end = System.currentTimeMillis();

		System.out.println("total time taken to insert bulk["+ rows+"] rows the batch = " + (end - start) + " ms");
		preparedStatement.close();

	}

	public void multipleRowsInsert(int rows) throws IOException, SQLException {
		List<List<String>> records = new ArrayList<>();
		String csvName = "BulkInsert-"+rows;
		BufferedReader br = new BufferedReader(new FileReader("src\\main\\resources\\BulkInsert-1000000.csv"));

		String line;
		int counter = 0;
		while ((line = br.readLine()) != null) {
			String[] values = line.split(",");
			records.add(Arrays.asList(values));
			counter++;
			if(counter == rows) { break ;}
		}

		if(rows < 1000) {
			String query = "INSERT INTO sales.customers Values";
			for (int i = 1; i < rows; i++) {
				for (int j = 1; j < records.get(i).size(); j++) {
					if (j == 1) {
						query += "('" + records.get(i).get(j) + "','";
					} else if (j < records.get(i).size() - 1) {
						query += records.get(i).get(j) + "','";
					} else {
						query += records.get(i).get(j) + "'),";
					}
				}
			}
			query = query.substring(0, query.length() - 1);
			long start = System.currentTimeMillis();
			st.execute(query);
			long end = System.currentTimeMillis();
			System.out.println("total time taken to insert multiple Rows [" + rows + "] rows the batch = " + (end - start) + " ms");
		} else {
			int iterator = 1 ;
			String allQueries = "";
			String query = "INSERT INTO sales.customers Values";
			for (int i = 1; i < rows; i++) {
				iterator++ ;

				for (int j = 1; j < records.get(i).size(); j++) {
					if (j == 1) {
						query += "('" + records.get(i).get(j) + "','";
					} else if (j < records.get(i).size() - 1) {
						query += records.get(i).get(j) + "','";
					} else {
						query += records.get(i).get(j) + "'),";
					}
				}

				if(iterator % 1000 ==0) {
					query = query.substring(0, query.length() - 1);
					allQueries += query + ";";
					query = "INSERT INTO sales.customers Values";
				}

			}
			if(rows % 1000 != 0 ) {
				if(query.substring(query.length()-1,query.length()).contains(",")) {
					System.out.println(query.substring(query.length()-2,query.length()-1));
					query = query.substring(0, query.length() - 1);
					allQueries += query + ";";
				} else {

				}
			}

			long start = System.currentTimeMillis();
			st.execute(allQueries);
			long end = System.currentTimeMillis();
			System.out.println("total time taken to insert multiple Rows [" + rows + "] rows the batch = " + (end - start) + " ms");
		}
	}

	@Test
	public void testQueryData() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.queryData("select top 10 * from [BikeStores].[sales].[customers]", test);
		test.queryData("select top 10 first_name, last_name, email from [BikeStores].[sales].[customers]", test);
		test.queryData("select top 30 * from production.products", test);
		test.connection.close();
	}

	@Test
	public void testUpdateData() throws SQLException {
		JDBCTest test = new JDBCTest();
		test.updateData("update production.products set list_price = 167.00 where product_id = 1",test);
		test.connection.close();
	}

	@Test
	public void testDeleteData() throws SQLException {
		JDBCTest test = new JDBCTest();
		test.deleteData("delete from production.stocks where store_id = 1 and product_id in (6,8) and quantity = 0",test);
		test.connection.close();
	}

	@Test
	public void testINsertData() throws SQLException {
		JDBCTest test = new JDBCTest();
		test.insertData("insert into production.stocks values(1,6,0),(1,8,0)", test);
		test.connection.close();
	}

	@Test
	public void testMultipleRowsInsert() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.multipleRowsInsert(10);
		test.connection.close();
	}

	@Test
	public void testBulkInsert() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.bulkInsert(1000);
		test.connection.close();
	}

	@Test
	public void testMultipleRowsInsertMultipleBatches() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.multipleRowsInsert(999);
		test.multipleRowsInsert(1000);
		test.multipleRowsInsert(10000);
		test.multipleRowsInsert(30000);
		test.multipleRowsInsert(50000);
		test.multipleRowsInsert(75000);
		test.multipleRowsInsert(100000);
		test.connection.close();
	}

	@Test
	public void testBulkInsertMultipleBatches() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.bulkInsert(999);
		test.bulkInsert(1000);
		test.bulkInsert(10000);
		test.bulkInsert(30000);
		test.bulkInsert(50000);
		test.bulkInsert(75000);
		test.bulkInsert(100000);
		test.connection.close();
	}

	@Test
	public void compareMultipleRowsWithBulkInsert() throws SQLException, IOException {
		JDBCTest test = new JDBCTest();
		test.multipleRowsInsert(100000);
		test.bulkInsert(100000);
		test.connection.close();
	}
}
