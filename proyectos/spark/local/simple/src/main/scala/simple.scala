import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Simple {
  def main(args: Array[String]) {
    val someTextFile = "/home/cotw/Cursos/hadoop/scala-2.11.7/program/simple/src/main/scala/simple.netflix"  //  This should be a file from one of your folder or hdfs 
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val fileRDD = sc.textFile(someTextFile)		
		println("Number of words %s".format(fileRDD.count()))
    }
}