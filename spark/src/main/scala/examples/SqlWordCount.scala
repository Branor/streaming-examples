package examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SqlWordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local[2]").setAppName("Test Application")
        val ssc = new StreamingContext(conf, Seconds(5))

        // Create a DStream that will connect to hostname:port
        val lines = ssc.socketTextStream("localhost", 1111, StorageLevel.MEMORY_AND_DISK_SER)

        // Split each line into words
        val words = lines.flatMap(_.split(" "))

        words.foreachRDD { rdd =>

            val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
            import sqlContext.implicits._

            // Convert the RDD to a DataFrame and register it as a table
            val wordsDataFrame = rdd.toDF("word")
            wordsDataFrame.registerTempTable("words")

            val wordCountsDataFrame = sqlContext.sql("SELECT   word, count(*) AS total " +
                                                     "FROM     words " +
                                                     "GROUP BY word ")
            wordCountsDataFrame.show()
        }

        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
    }
 }
