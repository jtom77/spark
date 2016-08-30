/**
  * Created by ThomasE on 08.03.2016.
  */

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "C:\\Users\\thomase\\Desktop\\log.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("INFO")).count()
    val numBs = logData.filter(line => line.contains("DEBUG")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
