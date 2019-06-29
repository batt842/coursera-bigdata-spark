package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings works") {
    import StackOverflow._

    val postings: RDD[Posting] = sc.parallelize(Seq(
      Posting(1, 0, Some(3), None, 0, None),
      Posting(1, 1, None, None, 0, None),
      Posting(2, 2, None, Some(0), 0, None),
      Posting(2, 3, None, Some(1), 0, None),
      Posting(2, 4, None, Some(1), 0, None)));

    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(postings)
    grouped.collect().foreach(r => {
      println(r._1)
      r._2.foreach(pair => println(pair._1.id + " : " + pair._2.id))
    })
  }

  test("clusterResults"){
    // https://www.coursera.org/learn/scala-spark-big-data/programming/FWGnz/stackoverflow-2-week-long-assignment/discussions/threads/_vR7RBTEEeejzQokTe8yRg
    import StackOverflow._

    val centers = Array((0,0), (100000, 0))
    val rdd = sc.parallelize(List(
      (0, 1000),
      (0, 23),
      (0, 234),
      (0, 0),
      (0, 1),
      (0, 1),
      (50000, 2),
      (50000, 10),
      (100000, 2),
      (100000, 5),
      (100000, 10),
      (200000, 100)  ))
    val result: Array[(String, Double, HighScore, HighScore)] = testObject.clusterResults(centers, rdd).sortBy(_._4)
    assert(result(0) === ("PHP", 75.0, 4, 5))
    assert(result(1) === ("JavaScript", 75.0, 8, 12))
  }
}
