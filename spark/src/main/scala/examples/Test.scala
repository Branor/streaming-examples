package examples

//import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]) {
        val file = "spark/src/main/resources/data.txt"
        val conf = new SparkConf().setMaster("local[2]").setAppName("Test Application")
        val sc = new SparkContext(conf)
        val data = sc.textFile(file, 2).cache()
        val wordCounts = data.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
        val counts = wordCounts.collect()
        println("Word Counts:")
        counts.foreach(print)
        println("")
    }
}
