package db.connect;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
public class DumpData {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/jmdb"+
			"?verifyServerCertificate=false"+ "&useSSL=false"+ "&requireSSL=false";

	//  Database credentials
	static final String USER = "root";
	static final String PASS = "mongodb";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try{

			// Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL,USER,PASS);

			//Execute a query
			stmt = conn.createStatement();
			String sql;
			sql = "show tables";   //get all name of the tables
			ResultSet rs = stmt.executeQuery(sql);
			ArrayList<String>tables=new ArrayList<String>();
			while(rs.next())
			{
				tables.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
			String collectionName= "";
			for(String table: tables)
			{
				System.out.println("Populating table: "+table);
				if(!table.startsWith("movies2"))       //merge tables like actors and movies2actors
				{
					collectionName =table;
				}
				else
				{
					collectionName= table.substring("movies2".length(),table.length());
					for(String table1: tables)
					{
						if(table1.startsWith(collectionName))
						{
							collectionName =table1;
						}
					}
				}

				Statement stmt1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
				stmt1.setFetchSize(Integer.MIN_VALUE);  //fetch only minimum number of rows
				sql = "select * from "+table;
				ResultSet rs1 = stmt1.executeQuery(sql);
				convert(rs1,collectionName);
				System.out.println("Table "+table+" loaded into collection: "+collectionName);
				rs1.close();
				stmt1.close();
			}

			conn.close();

		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}//end finally try
		}//end try
	}//end main


	public static void convert( ResultSet rs,String collectionName)
			throws SQLException
	{
		int limit =5000;  //a good value of limit is important. This ensure how many rows will be inserted in one shot.
		int docCount=0;
		List<Document> docs = new ArrayList<Document>();
		ResultSetMetaData rsmd = rs.getMetaData();

		while(rs.next()) {
			int numColumns = rsmd.getColumnCount();
			Document obj = new Document();

			for (int i=1; i<numColumns+1; i++) {
				String column_name = rsmd.getColumnName(i);  //replace movieid with movie title and year in all collections except in movies and akatitles  
				if(column_name.equalsIgnoreCase("movieid") && (!collectionName.equalsIgnoreCase("movies")
						&& !collectionName.equalsIgnoreCase("akatitles")))
				{
					Document movieDetailsDoc=getMovieDetails(rs.getInt(column_name));
					obj.put("movie", movieDetailsDoc);
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){
					obj.put(column_name, rs.getArray(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){
					obj.put(column_name, rs.getInt(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){
					obj.put(column_name, rs.getBoolean(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){
					obj.put(column_name, rs.getBlob(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){
					obj.put(column_name, rs.getDouble(column_name)); 
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){
					obj.put(column_name, rs.getFloat(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){
					obj.put(column_name, rs.getInt(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){
					obj.put(column_name, rs.getNString(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR){
					obj.put(column_name, rs.getString(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){
					obj.put(column_name, rs.getInt(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){
					obj.put(column_name, rs.getInt(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.DATE){
					obj.put(column_name, rs.getDate(column_name));
				}
				else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){
					obj.put(column_name, rs.getTimestamp(column_name));   
				}
				else{
					obj.put(column_name, rs.getObject(column_name));
				}
			}

			docs.add(obj);
			docCount++;     //row count in RDBMS
			if(docCount==limit)
			{
				insertIntoMongoDB(docs,collectionName);  //insert rows in mongodb
				docs=null;
				docs = new ArrayList<Document>();
				docCount=0;
				System.gc();    //call to garbage collector to sweep out any unused object
			}

		}
		if(docCount!=0)            //last chunk which can be less than 5000 documents
		{
			insertIntoMongoDB(docs,collectionName);
			docs=null;
			docs = new ArrayList<Document>();
			docCount=0;
			System.gc();
		}
	}


	public static void insertIntoMongoDB(List<Document> docs,String collectionName)
	{
		MongoClient mongo = null;
		try {

			mongo = new MongoClient("localhost", 27017);
			MongoDatabase db = mongo.getDatabase("imdb"); //this could be changed to point to any database
			MongoCollection<Document> mongocollection = db.getCollection(collectionName);
			mongocollection.insertMany(docs);	

		} catch (MongoClientException e) {
			e.printStackTrace();
		}
		finally {
			if(mongo!=null)
			{
				mongo.close();
			}
		}
	}

	//its better to get data from database rather than storing it in the memory(like hashtable) which may cause memory issues
	public static Document getMovieDetails(int movieid) //return document to be embedded for each movieid
	{
		Document movieDetailsDoc=null;
		Statement stmt = null;
		Connection conn =null;
		try
		{
			conn = DriverManager.getConnection(DB_URL,USER,PASS);
			stmt = conn.createStatement();
			String sql;

			sql = "select title, year from movies where movieid="+movieid;
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next())
			{
				
				movieDetailsDoc=new Document();
				movieDetailsDoc.put("movietitle", rs.getString(1));
				movieDetailsDoc.put("year", rs.getString(2));
			}
		}catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		}finally{
			//finally block used to close resources
			try{
				if(stmt!=null)
					stmt.close();
			}catch(SQLException se2){
			}// nothing we can do
			try{
				if(conn!=null)
					conn.close();
			}catch(SQLException se){
				se.printStackTrace();
			}
		}//end try
		return movieDetailsDoc;
	}
}
