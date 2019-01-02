package com.jana.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext

object CustomerSpending {
  
  def splitLines(line: String): (Int, Float) = {
    val values = line.split(",")
    (values(0).toInt, values(2).toFloat)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[3]","CustomerOrders")
    
    val customerOrders = sc.textFile("../customer-orders.csv")
    
    val customerOrdersRdd = customerOrders.map(x => splitLines(x))
    
    val customerSpendingRdd = customerOrdersRdd.reduceByKey((x, y) => x + y)
    
    customerSpendingRdd.map(c => c.swap).sortByKey(false, numPartitions = 1).map(c => c.swap).foreach(println)
    
    sc.stop()
  }
  
}