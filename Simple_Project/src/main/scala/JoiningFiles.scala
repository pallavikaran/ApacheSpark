import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory;
import java.io.File


object GumGum {  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GumGum").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //Assets
    val rawRdd = sc.textFile("/home/pkaran/Documents/dataset/assets")
    val jsonRdd = rawRdd.map(_.substring(32))
    val json = jsonRdd.map(x => x.replace("}, {", ","))
    val df = sqlContext.read.json(json)

    //Ad
    val rawRdd1 = sc.textFile("/home/pkaran/Documents/dataset/ad_events")
    val jsonRdd1 = rawRdd1.map(_.substring(32))
    val json1 = jsonRdd1.map(x => x.replace("}, {", ","))
    val df1 = sqlContext.read.json(json1)

    df.toDF().registerTempTable("df")
    df1.toDF().registerTempTable("df1")

    val joined = sqlContext.sql("""SELECT ass.pv,COUNT(ass.pv),
                                CASE WHEN ad.e='view' THEN COUNT(ad.e) ELSE 0 END,
                                CASE WHEN ad.e='click' THEN COUNT(ad.e) ELSE 0 END
                                FROM df ass JOIN df1 ad ON ass.pv=ad.pv   
                                GROUP BY ass.pv, ad.e""")

    //joined.collect().foreach(println)
     var collected=joined.coalesce(1)

    collected.rdd.saveAsTextFile("/home/pkaran/Documents/output6")

     sc.stop
  }
}
