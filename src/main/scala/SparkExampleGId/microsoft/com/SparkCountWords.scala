package SparkExampleGId.microsoft.com

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkCountWords {

  def main(args: Array[String]):Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("SparkCountWords").setMaster("local[4]").set("spark.executor.memory","1g"))

    sc.setLogLevel("ERROR")

    val allWords = sc.textFile("C:\\Users\\v-jaygu\\Desktop\\Onboarding.txt").flatMap(s => s.split(" "))

    val wordCounts = allWords.map(x => (x, 1)).reduceByKey((x, y) => (x + y))

    for(word <- wordCounts)
    {
      System.out.println("\""+ word._1 +"\": "+word._2 )
    }

    val maxWord = wordCounts.max()(new Ordering[Tuple2[String, Int]](){
      override def compare(x: (String, Int), y: (String, Int)): Int = Ordering[Int].compare(x._2, y._2)
    })
    System.out.println("*****************************************************************")
    System.out.println("Maximum times repeated word \"" + maxWord._1 + "\": " + maxWord._2)
    System.out.println("*****************************************************************")

    val sortedByCount = wordCounts.sortBy(x => x._2, ascending = true).collect()

    for(word1 <- sortedByCount)
    {
      System.out.println("\""+ word1._1 +"\": "+word1._2 )
    }
  }
}
