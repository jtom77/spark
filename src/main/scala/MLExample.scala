import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by ThomasE on 09.03.2016.
  */
object MLExample {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\Users\\thomase\\Downloads\\ml-100k\\u.user")
    val fields = data.map(line => line.split('|'))
    val users = fields.map(f => f(0)).count()
    val genders = fields.map(f => f(2)).distinct().count()
    val occupations = fields.map(f =>  f(3)).distinct().count()
    val zipCodes = fields.map(f => f(4)).distinct().count()
    printf("Users: %d, genders: %d, occupations: %d, ZIP codes: %d%n", users, genders, occupations, zipCodes)

  }
}
