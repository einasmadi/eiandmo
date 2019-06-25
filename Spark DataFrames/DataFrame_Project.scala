// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!
import org.apache.spark.sql.SparkSession
val spark =  SparkSession.builder().getOrCreate()

// Start a simple Spark Session

// Load the Netflix Stock CSV File, have Spark infer the data types.
val df = spark.read.option("header", "true").option("inferSchema", true).csv("Netflix_2011_2016.csv")
// What are the column names?
df.columns
// What does the Schema look like?
df.printSchema
// Print out the first 5 columns.
df.head(5)
// Use describe() to learn about the DataFrame.
df.describe()
// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val df2 = df.withColumn("HV Ration",df("High")/df("Volume"))
// What day had the Peak High in Price?
df.select(max("High")).show()
// What is the mean of the Close column?
df.select(avg("Close")).show()
// What is the max and min of the Volume column?
df.select(min("Volume"), max("Volume")).show()
// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?
df.filter($"Close" < 600).count()
// What percentage of the time was the High greater than $500 ?
df.filter($"High" > 500).count()/(df.count() + 0.0) * 100
// What is the Pearson correlation between High and Volume?
df.select(corr($"High", $"Volume")).show()
// What is the max High per year?
df.groupBy(year($"Date")).max("High").show()
// What is the average Close for each Calender Month?
df.groupBy(month($"Date")).avg("Close").orderBy("month(Date)").show()
