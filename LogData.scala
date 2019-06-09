
import org.apache.spark.SparkContext
import scala.collection.mutable.LinkedList
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.io.Source
import org.apache.spark.sql.hive.HiveContext;
 
//This is a sample code to read the log.txt file and extract errors and store it in hadoop environment in hive table using scala with JAVA APIs.

object LogData {
  
    def main(args: Array[String]) 
	{
												// Main function 
      	println("Hello, Harshit!") 
      
      val sc = new SparkContext("local[*]", "Count") 						//creating spark context/object providing application
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)				//creatin sql context/object using spark context
      val spark = org.apache.spark.sql.SparkSession.builder.appName("SampleKMeans") 
             .master("local[*]")
             .getOrCreate()									//Initiating the sparksession providing app name
             import spark.implicits._

        val source = Source.fromFile("C:\\Users\\HBUNG\\Desktop\\testlog.log")			//reading log file(source file) using fromFile function
        var dateTime, dateTime1 ,dateTime2=""							// date variables
        
        var ExceptionList = new LinkedList[String]
        ExceptionList = LinkedList("Exception/Error")
        var DateList = new LinkedList[String]
        DateList = LinkedList("Date")
        var TimeList = new LinkedList[String]
        TimeList = LinkedList("Time")
  for(line <- source.getLines()){								// for loop to read line by line from source variable and 		
    
  if (line.contains("AM") || line.contains("PM")){						//in each line search time and split it to data n time.
       dateTime = line.substring(0, 24).toString()
       dateTime1 = dateTime.substring(0,12)
       dateTime2 = dateTime.substring(13,24)
    }
    if(line.contains("Exception:"))								// after time search for exception and save the type of exception with date and time
        {  
        
          var exceptiontype = line.split(":")
      /*println(exceptiontype(0))
      println(dateTime)
      println(dateTime1)
      println(dateTime2)*/

    ExceptionList append LinkedList(exceptiontype(0))						// append all the exception and timeline in the linkedlist
    DateList append LinkedList(dateTime1)
    TimeList append LinkedList(dateTime2)
         
    }
   
  
} 
      	
       val ABCDzip =  ExceptionList.zip(DateList).zip(TimeList)					//zip is the function to form the tupples
       val ABCDtup = ABCDzip.map{case ((w,x),y)=>(w,x,y)}					// transpose the tupples into units
       val ABCDdf = ABCDtup.toDF("ExceptionList","DateList","DateList")				// convert thie list into data frame and assign column names
       ABCDdf.show
      // ABCDdf.write().mode(SaveMode.Overwrite).saveAsTable("harshit.Logsdata");		// saving in hive default data base
   
	source.close()											//stop all the connection and contexts
 

      
       sc.stop()
  
    }
}