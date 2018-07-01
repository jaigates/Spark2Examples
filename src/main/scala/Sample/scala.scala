package Sample


import org.apache.spark._
import org.apache.spark.sql._

object scala {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.parquet.binaryAsString","true")
    //spark.conf.set("","")
    spark.conf.set("spark.debug.maxToStringFields","100")
    val csvDf = spark.read.option("header", "true").csv("C:/Users/jaiga/Downloads/tools/bigdata/sample_data/2008.csv")
    csvDf.repartition(csvDf("year"), csvDf("month")).write.mode("overwrite").parquet("C:/Users/jaiga/Downloads/tools/bigdata/sample_data/2008_csv_repartitioned.parquet")
    

  }

}