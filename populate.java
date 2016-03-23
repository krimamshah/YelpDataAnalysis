
package yelp_gui;


//package YELP_DataAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class populate {

    private static JSONParser parser;
    private static Connecting_DB dbConnect;
    private static String[] businessCategories;
    private static String checkinJson =null;
    
    public populate(){
        dbConnect = new Connecting_DB();
        parser = new JSONParser();
    }
    public String[] getBusCategories(){
        String[] temp = {"Active Life","Arts & Entertainment","Automotive","Car Rental",
                            "Cafes","Beauty & Spas","Convenience Stores","Dentists","Doctors","Drugstores","Department Stores",
                            "Education","Event Planning & Services","Flowers & Gifts","Food","Health & Medical","Home Services",
                            "Home & Garden","Hospitals","Hotels & Travel","Hardware Stores","Grocery","Medical Centers",
                            "Nurseries & Gardening","Nightlife","Restaurants","Shopping","Transportation"};
        return temp;
    }
    
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, SQLException {
		 deleteData();
                 // populate data in DB 
         
        for(int i=0;i<args.length;i++){
         if(args[i].equals("yelp_user.json"))
             
             insertUserData(args[i]);
         else if(args[i].equals("yelp_business.json"))
             
            insertBusinessData(args[i]);
         else if(args[i].equals("yelp_checkin.json"))
            
           insertCheckInData(args[i]);
         else
             
            insertReviewData(args[i]);
        }
                
            }
        
         private static void deleteData() throws SQLException, ClassNotFoundException { 

           dbConnect = new Connecting_DB();
           Connection con =dbConnect.openConnection();
           Statement stmt = con.createStatement(); 

          stmt.executeUpdate("DELETE FROM BUSINESS_CATEGORY");

          stmt.executeUpdate("DELETE FROM YELP_REVIEW");

          stmt.executeUpdate("DELETE FROM YELP_CHECKIN");

          stmt.executeUpdate("DELETE FROM YELP_USER");

          stmt.executeUpdate("DELETE FROM YELP_BUSINESS");

           

          // System.out.println("Tuples deleted...now populate new tuples!!");

           

           stmt.close(); 
           dbConnect.closeConnection(con);

       }

	
        public static void insertCheckInData(String fileName) throws SQLException, ClassNotFoundException, ParseException{
             String jsonData = "";
		BufferedReader br = null;
		dbConnect = new Connecting_DB();
                parser = new JSONParser();
                 
                try {
			String line;
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
                        Connection con = dbConnect.openConnection();
			//long count = 0;
			while(line!=null)
			{
                        jsonData += line + "\n";
			Object obj = parser.parse(jsonData);
			JSONObject jsonObject = (JSONObject) obj;
			
                        //String type = (String) jsonObject.get("type");
			//String business_id = (String)jsonObject.get("business_id");
			//String checkin_info =""+jsonObject.get("checkin_info")+"";
                        JSONObject checkin_info = (JSONObject)jsonObject.get("checkin_info");
                        Set<String> set = checkin_info.keySet();
                        ArrayList<String> list  = new ArrayList<String>(set);

                         for(int i = 0 ; i < list.size() ; i++)
                         {
                          String business_id   = (String) jsonObject.get("business_id");
                          String type   = (String)jsonObject.get("type");
                          String listItem       = list.get(i);
                          String[] tokens  = listItem.split("-"); 
                          //long from_hour          = Long.valueOf(tokens[0]);
                          //long to_hour            = from_hour +1 ;
                          //long day              = Long.valueOf(tokens[1]);
                          long checkin_count      = (long)checkin_info.get(listItem);
                          long hour = Long.valueOf(tokens[0]);
                          long day = Long.valueOf(tokens[1]);
                          //long checkin= (long)checkin_info.get(list);
                         
                                
                        String insertCheckin = "INSERT INTO YELP_CHECKIN VALUES(?,?,?,?,?)";
			PreparedStatement pst  = con.prepareStatement(insertCheckin);
                        //pst.setLong(1, count);
                        pst.setString(1,type);
			pst.setString(2,business_id);
                        pst.setLong(3,hour);
                        pst.setLong(4, day);
                        pst.setLong(5, checkin_count);
                       // pst.setString(3,checkin_info);
                        pst.executeUpdate();
                        pst.close();
                        
                        }
                        //count++;
			line = br.readLine();
			jsonData = "";
			}
			
			br.close();
			dbConnect.closeConnection(con);
		}catch(IOException ex){
			ex.printStackTrace();
		}
         }
         public static void insertReviewData(String fileName) throws SQLException, ClassNotFoundException, ParseException{
             String jsonData = "";
		BufferedReader br = null;
		dbConnect = new Connecting_DB();
                parser = new JSONParser();
                 
                try {
			String line;
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
                        Connection con = dbConnect.openConnection();
			
			while(line!=null)
			{
                        jsonData += line + "\n";
			Object obj = parser.parse(jsonData);
			JSONObject jsonObject = (JSONObject) obj;
                        
                        JSONObject votes = (JSONObject) jsonObject.get("votes");
                        long useful = (long)votes.get("useful");
                        long funny = (long) votes.get("funny");
                        long cool = (long) votes.get("cool");
		        String user_id = (String) jsonObject.get("user_id");
                        String review_id = (String) jsonObject.get("review_id");
			String business_id = (String)jsonObject.get("business_id");
                        String type = (String)jsonObject.get("type");
                        String review_date = (String) jsonObject.get("date");
			String text = (String) jsonObject.get("text");
			long stars =(long) jsonObject.get("stars");
                       
                        String insertReview = "INSERT INTO YELP_REVIEW VALUES(?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement pst  = con.prepareStatement(insertReview);
                        pst.setLong(1,useful);
			pst.setLong(2,funny);
                        pst.setLong(3,cool);
                        pst.setString(4,user_id);
                        pst.setString(5,review_id);
                        pst.setLong(6,stars);
                        pst.setString(7,review_date);
                        pst.setString(8,text);
                        pst.setString(9,type);
                        pst.setString(10,business_id);
                        pst.executeUpdate();
                        pst.close();
                     
			line = br.readLine();
			jsonData = "";
			}
			
			br.close();
			dbConnect.closeConnection(con);
		}catch(IOException ex){
			ex.printStackTrace();
		}
         }
         public static void insertBusinessData(String fileName) throws SQLException, ClassNotFoundException, ParseException{
             String jsonData = "";
		BufferedReader br = null;
                dbConnect = new Connecting_DB();
                 parser = new JSONParser();
                 
                try {
			String line;
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
                        populate populateBusCategory = new populate();
			Connection con =  dbConnect.openConnection();
                        PreparedStatement pst;
			String comma = ",";
                       // int count = 0;
			while(line!=null)
			{
                        jsonData += line + "\n";
			Object obj = parser.parse(jsonData);
			JSONObject jsonObject = (JSONObject) obj;
			
                        String name = "'"+jsonObject.get("name")+"',";
			String business_id = (String)jsonObject.get("business_id");
			String city = (String) jsonObject.get("city");
                        String state = (String) jsonObject.get("state");
                        double stars = (double)jsonObject.get("stars");
			String type = "'"+jsonObject.get("type")+ "'";
                        String insertBusiness = "INSERT INTO YELP_BUSINESS VALUES(?,?,?,?,?,?)";
			pst  = con.prepareStatement(insertBusiness);
                        pst.setString(1,business_id);
			//pst.setString(2,subcategories);
                        pst.setString(2,city);
                        pst.setString(3,state);
                        pst.setString(4,name);
                        pst.setDouble(5,stars);
                        pst.setString(6,type);
                        pst.executeUpdate();
		        pst.close();		
		
                        //String businessCategory = null;
		        JSONArray categories = (JSONArray)jsonObject.get("categories");
                        //String tempCategories = (String) categories.get(0);
			/*for(int i=0;i<businessCategories.length;i++){
                            if(tempCategory.equals(businessCategories[i]))
                                 businessCategory = "'"+tempCategory+"'";               
                        }*/
                        String insertStmt = "INSERT INTO BUSINESS_CATEGORY VALUES(?,?,?)" ;
                                pst = con.prepareStatement(insertStmt);
                               // pst.setInt(1,count);
                                pst.setString(1, business_id);
                        boolean isBusCategory;
                      Vector<String> category = new Vector<String>();
                      Vector<String> subcategory = new Vector<String>();
                      for (int i = 0; i < categories.size() ; i++){
                           isBusCategory = false;
                          String tempCategory =(String)categories.get(i);
                          for(int j=0;j<populateBusCategory.getBusCategories().length;j++){
                           if(populateBusCategory.getBusCategories()[j].contains(tempCategory)){
                               category.add((String) categories.get(i));
                                  isBusCategory= true;
                           }
                           //else{
                             //    subcategory.add((String) categories.get(i));
                               // }
                           }
                           if(!isBusCategory)
                           subcategory.add((String) categories.get(i));
                           
                        }
                      
                       /* for(int i = 0 ; i < category.size() ; i++){
                            for(int j=0;j<subcategory.size();j++){
                           
                                
                                pst.setString(2, category.get(i));
                                pst.setString(3, subcategory.get(j));
                                pst.executeUpdate();
                                pst.close();
                            }
                            
                        }*/
                      
                      for (String c: category){
        	pst.setString(2,  c);
        	if(subcategory.isEmpty()){
        		String trash=null;
        		pst.setString(3,  trash);
        		pst.executeUpdate();
        	}
        	else{
        		for (String s: subcategory){
        			pst.setString(3, s);
        			pst.executeUpdate();
        		}
        		
        	}
        }
                        pst.close();
			line = br.readLine();
			jsonData = "";
			}
			
			br.close();
			dbConnect.closeConnection(con);
		}catch(IOException ex){
			ex.printStackTrace();
		}    
                        
         }
        
        public static void insertUserData(String fileName) throws SQLException, ClassNotFoundException, ParseException{
            String jsonData = "";
		BufferedReader br = null;
		//JSONParser parser = new JSONParser();
                parser = new JSONParser();
		try {
			String line;
			br = new BufferedReader(new FileReader(fileName));
			line = br.readLine();
			
			Connecting_DB dbConnect = new Connecting_DB();
			Connection con = dbConnect.openConnection();
			String comma = ",";
			while(line!=null)
			{
			jsonData += line + "\n";
			Object obj = parser.parse(jsonData);
			JSONObject jsonObject = (JSONObject) obj;
		
			//Object o =  jsonObject.get("compliments");
			//System.out.println(jsonObject.get("compliments"));
			
			String yelping_since = (String) jsonObject.get("yelping_since");
			String votes = ""+jsonObject.get("votes")+"";
			//String review_count = "'"+jsonObject.get("review_count")+"',";
                        long review_count = (long)jsonObject.get("review_count");
			String name = (String)jsonObject.get("name");
			String user_id = (String)jsonObject.get("user_id");
			//String friends = "'" +jsonObject.get("friends")+"',";
		  JSONArray frnds = (JSONArray)jsonObject.get("friends");
			int friends = frnds.size();
			/*Iterator<String> iterator = frnds.iterator();
			while(iterator.hasNext()){
				friends += iterator.next()+",";
			}
			friends += "',";*/
			
			//String fans = "'"+jsonObject.get("fans")+"',";
                        long fans = (long)jsonObject.get("fans");
                        
			//String average_stars = "'"+jsonObject.get("average_stars")+ "',";
                        double average_stars = (double)jsonObject.get("average_stars");
			String type = (String)jsonObject.get("type");
			//String compliments = "'"+jsonObject.get("compliments")+ "',";
			String elite = ""+ jsonObject.get("elite")+"";
			
			/*JSONArray elt = (JSONArray)jsonObject.get("elite");
			String elite = "'";
			Iterator<String> iterator1 = elt.iterator();
			while(iterator1.hasNext()){
				elite += iterator1.next()+",";
			}
			elite += "'";*/
			
			//String user = yelping_since +votes+review_count+comma+ name +user_id+friends+fans+comma+average_stars+comma+type+elite;
			//String insertUser ="INSERT INTO YELP_USER(yelping_since,votes,review_count,name,user_id,numOfFriends,fans,average_stars,type,elite)"
			//	+ " VALUES("+user+")";

// +friends
                        String insertUser = "INSERT INTO YELP_USER VALUES(?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement pst  = con.prepareStatement(insertUser);
                        pst.setString(1,yelping_since);
			pst.setString(2,votes);
                        pst.setLong(3,review_count);
                        pst.setString(4,name);
                        pst.setString(5,user_id);
                        pst.setInt(6,friends);
                        pst.setLong(7,fans);
                        pst.setDouble(8,average_stars);
                        pst.setString(9,type);
                        pst.setString(10,elite);
                        
                        
                        pst.executeUpdate();
		        pst.close();
		//System.out.println(insert);
			
			//Connecting_DB dbConnect = new Connecting_DB(insert);
			//dbConnect.run();
			//dbConnect.run(insertUser);
			line = br.readLine();
			jsonData = "";
			}
			
			br.close();
			dbConnect.closeConnection(con);
		}catch(IOException ex){
			ex.printStackTrace();
		}
        }

}


