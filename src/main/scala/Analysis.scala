import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ListBuffer

/** IntelliJ IDEA
	* @author 280320889@qq.com <br>
	*/

class Analysis {
}
object Analysis{
	def main(args:Array[String]): Unit ={
			if(args.length !=3){
				println("Usage : java -jar ***.jar dependency_jars file_location save_location")
				System.exit(1)
			}
		val jars = ListBuffer[String]()
		args(0).split(',').map(jars += _)
		val conf = new SparkConf()
		conf.setMaster("spark://server1:8888")
						.setSparkHome(System.getenv("SPARK_HOME"))
						.setAppName("Analysis")
						.set("spark.executor.memory","25g")
		val sc = new SparkContext(conf)
		val data = sc.textFile(args(1))

		data.cache()
		println(data.count())
		data.filter(_.split(' ').length ==3).map(_.split(' ')(1)).map((_,1)).reduceByKey(_+_)
						.map(x => (x._2,x._1)).sortByKey(false).map(x=> (x._2,x._1)).saveAsTextFile(args(2))
	}
}
