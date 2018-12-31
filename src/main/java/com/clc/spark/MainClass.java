/**
 * 
 */
package com.clc.spark;

import java.io.File;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 * <b>Description:</b>
 * </p>
 * <p>
 * This class shows how to use Apache Spark and Java to read a CSV
 * file.
 * </p>
 * 
 * @author Paul W Nichols
 * @version 0.1 Beta
 *
 */
public class MainClass {
  private static final Logger LOG =Logger.getLogger(MainClass.class.getName());
  private static final String FILE_PATH="/resources/free-zipcode-database.csv";
  private static SparkSession spark;
  private static Dataset<Row> data;
  private static MainClass mainClass;
  private Dataset<Row>dfCityStateZip;
  private static final Scanner scanner = new Scanner(System.in);
  /*
   * 
   */
  public static void main (String args[]) {
	  LOG.info("Entering main ");
	  System.out.println("\n\tStarting System....");
	  if(mainClass==null) {
		  mainClass=new MainClass();
		  mainClass.createSparkSessionAndDataset();
	       mainClass.showMenu();
	  }
  }	  
  
  /**
   * Shows the Menu Options
   */
  private void showMenu() {
	  StringBuilder menu=new StringBuilder();
	  menu.append("\n\t\t=======================================================");
	  menu.append("\n\t\t= Spark 2.0 Datasets and DataFrame Example            =");
	  menu.append("\n\t\t= ----------------------------------------            =");
	  menu.append("\n\t\t=  1. Query By City/State                             =");
	  menu.append("\n\t\t=  2. Query By Zip Code                               =");
	  menu.append("\n\t\t=  3. Exit                                            =");
	  menu.append("\n\t\t=                                                     =");
	  menu.append("\n\t\t=======================================================");
	  System.out.printf(menu.toString());
	  System.out.printf("\n\t\t Enter the # of the command you wish to run: ");
	  String cmd=scanner.nextLine();
	  runCommand(cmd);

  }
  /**
   * 
   * @param cmd
   */
  private void runCommand(String cmd) {
	  int iCmd=0;
	  try {
		  iCmd=Integer.parseInt(cmd);
	  }
	  catch(NumberFormatException nfe) {
		  LOG.error("Command is invalid:"+cmd);
		  LOG.error("You must enter a valid option to execute");
		  
	  }
	  switch (iCmd) {
	     case 1:
	    	 queryByCityState();
		    break;
	     case 2:
	    	 queryByZipcode();
	    	 break;
	     case 3:
	    	 exitSystem();
	    	 break;
	     default:
	    	 break;
	  }
	  
  }
  
  /**
   * Run the queryByCityState
   */
  private void queryByCityState() {
	  System.out.printf("\n\t Enter City: ");
	  String city=scanner.nextLine();
	  System.out.printf("\n\t Enter State: ");
	  String state=scanner.nextLine();
	  final Dataset<Row> cityResult=dfCityStateZip.filter("City = '"+city.toUpperCase()+"'"+
			  " AND State='"+state.toUpperCase() + "'");
	  //Let's show all results by getting the cityResult.count
	  final int totalRecords=(int)cityResult.count();
	  System.out.printf("\nTotal Records Returned: %d\n",totalRecords);
	   cityResult.show(totalRecords);
	  sendToMenu(4000);
  }
  /**
   * Queries By Zip Code
   */
  private void queryByZipcode() {
	  System.out.printf("\n\t Enter ZipCode: ");
	  String zipcode=scanner.nextLine();
	  final Dataset<Row> zipResult=dfCityStateZip.filter("zipCode = '"+ zipcode +"'");
	  //Let's show all results by getting the cityResult.count
	   zipResult.show();
	   sendToMenu(2500);
  }
  
  private void sendToMenu(int sleepTime) {
	  
		 System.out.println("\n\tPress <ENTER> to continue.");
         scanner.nextLine();
         showMenu();
  }
  
  /**
   * This creates the initial SparkSession and DataSet from the RDD
   */
  private void createSparkSessionAndDataset() {
	    //Get the current file Context
	    File file= new File("");
	    String zipCodeFile=file.getAbsolutePath()+"/resource/free-zipcode-database.csv";
	    //Create a Spark Session and initialize the session
	    System.out.println("\n\tLoading Spark Session.... Please wait...");
		spark = SparkSession
              .builder()
              .appName("Spark SQL Example")
              //master is set to local and [*] tells the Spark Session to use all CPU cores
              .master("local[*]")
              .getOrCreate();
		//Load a CSV File
		try {
		 //We are creating the DataSet data, to read a csv file and to ignore malformed records.
		 data = spark.read().format("csv").option("header",true).option("mode","DROPMALFORMED").load(zipCodeFile);
		}
		catch(Exception e) {
			LOG.error("Error opening file and initializing DataSet");
			LOG.error(e);
		}
		/* Show the first 20 rows
		 data.show();
		*/
		/* Show the schema of the CSV file 
		 data.printSchema();
		*/ 
		//Let's do some queries
		 dfCityStateZip = data.select(("City"), ("State"), ("Zipcode"), ("Lat"), ("Long"));
		 // dfCityStateZip.show();
	 
    
  }
  private void exitSystem() {
	  spark.stop();
	  System.out.println("\n Exiting System.");
	  System.exit(0);
  }
	
}
