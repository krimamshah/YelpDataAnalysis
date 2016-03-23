
package yelp_gui;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import javax.swing.table.DefaultTableModel;
//import java.sql.Statement;

public class Connecting_DB extends Thread {
	private Connection con;
	private String insert;
        private int columns;
        private int rows;
        private String[] columnNames;
         private final DefaultTableModel dataModel;
         private String setDefine;
	
	public Connecting_DB(){
		this.con = null;
                dataModel = new DefaultTableModel();
               // this.columnNames = null;
                
		//this.insert = insert;
	}
        
        
        
        public int getColumns(){
            return this.columns;
        }
        public int getRows(){
            return this.rows;
        }
       /* public String[] getColumnNames(){
            return this.getColumnNames();
        }*/
	
        
       
        
	public synchronized void run(String insert){
		//Connection con = null;
		
		try{
			//con= openConnection();
			//System.out.println("DbConnection Open");
			this.insert = insert;
                        insertUserData(insert);
		}catch(SQLException e){
			System.out.println("Errors occurs when communicating with the database server: "+e.getMessage());
		}finally{
			//closeConnection();
			
		}
	}
	
	private synchronized void insertUserData(String insert) throws SQLException{
	try{
		PreparedStatement pst = con.prepareStatement(insert);
		pst.executeUpdate();
		//pst.executeUpdate("INSERT INTO YELP_USER1 VALUES("+insert+")");
		System.out.println("Insert Successful");
		//update query
		//st.executeUpdate("INSERT INTO YELP_USER VALUES('Y13','ksmith@yahoo.com','Kian','Smith',TO_DATE('12/12/1992','MM/DD/YYYY'),'FL','F','')");
		//System.out.println("Insert successful");
		//select query
		//ResultSet rs =  st.executeQuery("SELECT * FROM YELP_USER");
		//showResultSet(rs);
		pst.close();
		}catch(SQLException s){
			System.out.println("Insert unsuccessful: "+s);
		}	
	}
        
        public DefaultTableModel displayUserResults(String selectUser) throws SQLException{
            PreparedStatement pst = con.prepareStatement(selectUser);
            ResultSet rs =  pst.executeQuery();
            //DefaultTableModel displayResultData = showResultSet(rs);
            ResultSetMetaData metaData = rs.getMetaData();
                columns = metaData.getColumnCount();
                columnNames = new String[columns];
                for(int i=1;i<=columns;i++){
                    columnNames[i-1]= metaData.getColumnName(i);
                         
                }
                dataModel.setColumnIdentifiers(columnNames);
                
                //now populate the data
                while(rs.next()){
                    String[] rowData = new String[columns];
                    for(int i=1;i<=columns;i++){
                        rowData[i-1] = rs.getString(i);
                    }
                    dataModel.addRow(rowData);
                }
    
            rs.close();
            pst.close();
            return  dataModel;
        }
	
	/*private DefaultTableModel showResultSet(ResultSet result) throws SQLException{
		ResultSetMetaData metaData = result.getMetaData();
                columns = metaData.getColumnCount();
                columnNames = new String[columns];
                for(int i=1;i<=columns;i++){
                    columnNames[i-1]= metaData.getColumnName(i);
                         
                }
                dataModel.setColumnIdentifiers(columnNames);
                
                //now populate the data
                while(result.next()){
                    String[] rowData = new String[columns];
                    for(int i=1;i<=columns;i++){
                        rowData[i-1] = result.getString(i);
                    }
                    dataModel.addRow(rowData);
                }
    
                
                return dataModel;
		/*int tupleCount = 1;
		while(result.next()){
			System.out.println("Tuple"+ tupleCount++ +":");
			for(int col=1;col<=metaData.getColumnCount();col++){
				System.out.print("\""+result.getString(col)+"\",");
			}
			System.out.println();
		}
	}*/
	
	public Connection openConnection() throws SQLException,ClassNotFoundException{
		//Load the Oracle database driver
		/*DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
		OracleDataSource ods = new OracleDataSource();
		ods.setTNSEntryName("ORACLE");
		ods.setUser("scott");
		ods.setPassword("tiger");
		ods.setDriverType("oci");*/
		
		String host = "localhost";
		String port = "1522";
		  String dbName = "oracle"; 
		  String userName = "hr"; 
		  String password = "Kreemalav777"; 
		String dbURL = "jdbc:oracle:thin:@"+host+":"+port+":"+dbName;
		
		con =  DriverManager.getConnection(dbURL, userName, password);
		//System.out.println("DbConnection Open");
		 return con;
	}
	
	public void closeConnection(Connection con){
		try{
                    this.con = con;
			con.close();
			//System.out.println("DbConnection Closed");
		}catch(SQLException e){
			System.err.println("Cannot close connection"+ e.getMessage());
		}

}
}



