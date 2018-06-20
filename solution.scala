val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
import sqlContext.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


println("Step 1. Download a dataset (.csv file) from the site ")
val csv = sc.textFile("/user/cloudera/wordcount/nuclear.csv").cache()
case class Nuclear(nc_cost: Double, nc_date: String)
case class NuclearMean(nc_date: String, nc_mean: Double)
case class NuclearMeanVar(nc_date: String, nc_mean: Double, nc_variance: Double)
val headerAndRows = csv.map(line => line.split(",").map(_.trim)).cache()
val header = headerAndRows.first
val mtcdata = headerAndRows.filter(_(0) != header(0))





println("Step2. Select a categorical variable and a numeric variable and form the key-value pair and create a pairRDD called population.")
val nuclearRDD = mtcdata.map(p => Nuclear(p(1).toDouble, p(2))).toDF().cache()
nuclearRDD.printSchema



println("Step 3. Compute the mean mpg and variance for each category and display as shown below:")
println("Category Mean Variance")
def computeMeanVariance(nucMeanRDD:DataFrame, ts:String ) : (Double, Double) = {	
	   var sum1:Double = 0.00
	   var count:Int = 0
	   val mean = nucMeanRDD.filter(nucMeanRDD("nc_date").equalTo(ts)).select("nc_mean").collectAsList().get(0).getDouble(0)
	   val temp = nuclearRDD.filter(nuclearRDD("nc_date").equalTo(ts)).select("nc_cost").collectAsList()
	   for(fs <- temp){
		val cost = fs.getDouble(0)
		sum1 += math.sqrt((cost-mean)*(cost -mean))
		count += 1
	   }
	   val nucMeanVarRDD = nucMeanRDD.filter(nucMeanRDD("nc_date").equalTo(ts)).map(p => NuclearMeanVar(p(0).toString, p(1).toString.toDouble, sum1/count)).toDF().cache()
           return (mean.toString.toDouble, (sum1/count).toString.toDouble)		
   }
def computeMeanVarianceFinal(nucMeanRDD:DataFrame, ts:String ) : (Double, Double) = {
	   var sum1:Double = 0.00
	   var count:Int = 0
	   val mean = nucMeanRDD.filter(nucMeanRDD("nc_date").equalTo(ts)).select("nc_cost").collectAsList().get(0).getDouble(0)
	   val temp = nuclearRDD.filter(nuclearRDD("nc_date").equalTo(ts)).select("nc_cost").collectAsList()
	   for(fs <- temp){
		val cost = fs.getDouble(0)
		sum1 += math.sqrt((cost-mean)*(cost -mean))
		count += 1
	   }
	   val nucMeanVarRDD = nucMeanRDD.filter(nucMeanRDD("nc_date").equalTo(ts)).map(p => NuclearMeanVar(p(0).toString, p(1).toString.toDouble, sum1/count)).toDF().cache()
           return (mean.toString.toDouble, (sum1/count).toString.toDouble)		
   }
val nucMeanRDD = (nuclearRDD.groupBy("nc_date").agg(avg("nc_cost"))).map(p => NuclearMean(p(0).toString, p(1).toString.toDouble)).toDF().cache()
for(ts <- nucMeanRDD.select("nc_date").distinct().rdd.map(r => r(0)).collect()){
	val temp = computeMeanVariance(nucMeanRDD, ts.toString)
	println(ts.toString+ " "+ temp._1 + " "+temp._2)
}




println("Step 4. Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.")
val mtNuclearBoostrapperRDD = nuclearRDD.sample(false, 0.25)





println("Step 5. Do 1000 times")
println("Category Mean Variance")
val listVariance  = new ListBuffer[NuclearMeanVar]()
for { x <-1 to 100 } 
{  
	val mtResampledDate = mtNuclearBoostrapperRDD.sample(true, 1); 
	for(ts <- mtResampledDate.select("nc_date").distinct().rdd.map(r => r(0)).collect()){
		val temp = computeMeanVarianceFinal(mtResampledDate, ts.toString)
		val mv = NuclearMeanVar(ts.toString, temp._1, temp._2)
		listVariance += mv
		println(ts.toString+ " "+ temp._1 + " "+temp._2)
	}
}




println("Step 6. Divide each quantity by 1000 to get the average and display the result.")
def computeFinal(lastRDD:DataFrame, ts:String ) = {
	val meanAVG = lastRDD.filter(lastRDD("nc_date").equalTo(ts)).agg(avg("nc_mean")).collectAsList().get(0).getDouble(0)
	val varianceAVG = lastRDD.filter(lastRDD("nc_date").equalTo(ts)).agg(avg("nc_variance")).collectAsList().get(0).getDouble(0)
	println (ts.toString +" " + meanAVG.toString.toDouble +" " + varianceAVG.toString.toDouble)		
}
println("Category Mean Variance")
for(ts <- mtNuclearBoostrapperRDD.select("nc_date").distinct().rdd.map(r => r(0)).collect()){
	computeFinal(listVariance.toDF(), ts.toString)
}

