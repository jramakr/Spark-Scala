package com.jana.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext

object MostPopularSuperheroAlt {
  
  
  def parseNames(line: String): Option[(Int, String)] = {
    
    val fields = line.split('\"')
    
    if (fields.length > 1) Some(fields(0).trim().toInt, fields(1)) else None

  }
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[3]","MostPopularSuperheroAlt")
    
    val names = sc.textFile("../marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)
    
    val lines = sc.textFile("../marvel-graph.txt")
    
    val linesRdd = lines.map(x => {val ids = x.split("\\s+").map(x => x.trim().toInt); (ids(0), ids.drop(1).toSet)})

    val friendsByCharacter = linesRdd.reduceByKey((x, y) => x | y).map(x => (x._1, x._2.size))
    val topN = 10
    val topCharacters = friendsByCharacter.takeOrdered(10)(Ordering.by[(Int, Int), Int](_._2))
    println(s"The $topN superheroes are")
    for (c <- topCharacters) {
      val mostPopularName = namesRdd.lookup(c._1)(0)
      println(s"$mostPopularName with ${c._2} co-appearances.")
    }   
    
    sc.stop()
    
  }
  
}