//Define all transformation functions
object transformationFunctions {
      def rowToArray(item : String) : Array[String] = {
          item.split(",")
      }
      def removeBadRows(item : Array[String]) : Boolean = {
           !item.contains("") && item.length == 6
      }
      def groupByStoreId(item : Array[String]) : (String,Int) = {
          (item(0),item(5).toInt)
      }
      def groupByStoreAndSalesmanId(item : Array[String]) : ((String,String),Int) = {
          ((item(0),item(1)),item(5).toInt)
      }
      def groupByStoreAndProductId(item : Array[String]) : ((String,String),Int) = {
          ((item(0),item(2)),item(5).toInt)
      }
      def groupByProductId(item : Array[String]) : (String,Int) = {
          (item(2),item(5).toInt)
      }
}
//Define sum by keys function
object actionFunctions {
      def sumByKeys(a:Int,b:Int) : Int = {
          a+b
      }
}
//Read all files
val rdd = sc.textFile("/SPARK/data/data*")
//Create new variable containing only valid records
var vald = rdd.map(transformationFunctions.rowToArray).filter(transformationFunctions.removeBadRows)
//Group by store_id
var gb0 = vald.map(transformationFunctions.groupByStoreId).reduceByKey(actionFunctions.sumByKeys)
//Group by store_id, salesman_id 
var gb1 = vald.map(transformationFunctions.groupByStoreAndSalesmanId).reduceByKey(actionFunctions.sumByKeys)
//Group by store_id, product_id
var gb2 = vald.map(transformationFunctions.groupByStoreAndProductId).reduceByKey(actionFunctions.sumByKeys)
//Group by product_id
var gb3 = vald.map(transformationFunctions.groupByProductId).reduceByKey(actionFunctions.sumByKeys)
//Save to files
gb0.saveAsTextFile("/SPARK/output/groupA")
gb1.saveAsTextFile("/SPARK/output/groupB")
gb2.saveAsTextFile("/SPARK/output/groupC")
gb3.saveAsTextFile("/SPARK/output/groupD")
//Calculate total number of records
var sumAll = rdd.count
//Calculate valid records
var sumValid = vald.count
//Calculate bad records
var sumBad = sumAll-sumValid
//Print numbers
println("Total number of records: "+sumAll)
println("Number of bad records: "+sumBad)
println("Number of records used for the cube: "+sumValid)
System.exit(0)
