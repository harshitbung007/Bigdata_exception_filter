import org.apache.spark.SparkContext
import scala.collection.mutable.LinkedList
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.io.Source
import org.apache.spark.sql.hive.HiveContext;
 


object LogData {
  
    def main(args: Array[String]) {
      println("Hello, Harshit!")
      
      val sc = new SparkContext("local[*]", "Count")
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      val spark = org.apache.spark.sql.SparkSession.builder.appName("SampleKMeans") 
             .master("local[*]")
             .getOrCreate()
             import spark.implicits._

        val source = Source.fromFile("C:\\Users\\HBUNG\\Desktop\\testlog.log")
        var dateTime, dateTime1 ,dateTime2=""
        
        var ExceptionList = new LinkedList[String]
        ExceptionList = LinkedList("Exception/Error")
        var DateList = new LinkedList[String]
        DateList = LinkedList("Date")
        var TimeList = new LinkedList[String]
        TimeList = LinkedList("Time")
  for(line <- source.getLines()){
    
  if (line.contains("AM") || line.contains("PM")){
       dateTime = line.substring(0, 24).toString()
       dateTime1 = dateTime.substring(0,12)
       dateTime2 = dateTime.substring(13,24)
    }
    if(line.contains("Exception:"))
        {  
        
          var exceptiontype = line.split(":")
      /*println(exceptiontype(0))
      println(dateTime)
      println(dateTime1)
      println(dateTime2)*/

    ExceptionList append LinkedList(exceptiontype(0))
    DateList append LinkedList(dateTime1)
    TimeList append LinkedList(dateTime2)
         
    }
   
  
} 
       println(ExceptionList) 
       println(DateList) 
       println(TimeList) 
       val ABCDzip =  ExceptionList.zip(DateList).zip(TimeList)
       val ABCDtup = ABCDzip.map{case ((w,x),y)=>(w,x,y)}
       val ABCDdf = ABCDtup.toDF("ExceptionList","DateList","DateList")
       ABCDdf.show
      // ABCDdf.write().mode(SaveMode.Overwrite).saveAsTable("harshit.Logsdata");
   
source.close()
 

      
       sc.stop()
  
    }
}