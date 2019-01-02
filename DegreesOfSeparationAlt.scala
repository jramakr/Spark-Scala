package com.jana.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import org.apache.spark.rdd._

object DegreesOfSeparationAlt {
  
  type ProcessedNodes = (Array[Int], Int)
  
  var foundDestId: Option[LongAccumulator] = None
  var numIter: Option[LongAccumulator] = None
  
  def parseInputLine(line:String): (Int, Array[Int]) = {
    val input = line.split("\\s+")
    (input(0).toInt, input.drop(1).map(v => v.toInt))
  }
  
  def checkIfDestIdExists(inId: Int, destId: Int): Boolean = {
    if (foundDestId.isDefined && inId == destId){
      foundDestId.get.add(1)
      true
    } else false
  }
  
  def getNextLevel(sc: SparkContext, inRDD: RDD[(Array[Int])]): RDD[(Array[Int])] = {
    ???
  }
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[3]","DoSAlt")

    foundDestId = Some(sc.longAccumulator("Found Dest"))
    numIter = Some(sc.longAccumulator("Num Iter"))
    
    
    val srcId: Int = 5306
    val destId: Int = 14
    
    val srcData = sc.textFile("../Marvel-graph.txt").map(parseInputLine)
    val keyAsList = srcData.map(x => (x._1, x._2))
    
    //Consolidate descendants of source key -- 1 degree of separation
    var consolidatedNodesRDD = keyAsList.filter(x => x._1 == srcId)
                                .reduceByKey((x, y) => x ++ y)
                                .map(x => (List(x._1), x._2))    

    var currentList = consolidatedNodesRDD.flatMapValues(x => x)
    
    foundDestId.get.add(1)
    while (foundDestId.get.value < 2 && numIter.get.value < 10){
      
      //flatten current level & check if the latest level contains dest
      val initialList = currentList.map(x => ((x._2 :: x._1), checkIfDestIdExists(x._2, destId)))
      
      // Debug 1:
      println(s"Iteration ${numIter.get.value}: ${initialList.count()} entries.")
      
      if (foundDestId.get.value > 1){
        val foundMatch = initialList.filter(x => x._2)
        foundMatch.foreach(println)
      }
      else {
        //val nextLevel = initialList.map(x => (x._1,keyAsList.lookup(x._1(0)).flatMap(x => x)))
        val nextLevel = initialList.map(x => (x._1(0), (x._1))).join(keyAsList).map(x => (x._2._1, x._2._2))
        currentList = nextLevel.flatMapValues(x => x)
      }
                
      numIter.get.add(1)
    }
    
    
    sc.stop()
  }
}