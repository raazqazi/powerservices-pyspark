from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from powerservice import trading
import sys
from datetime import datetime
dateTime = datetime.now().strftime("%Y%M%d_%H%M")

if __name__=='__main__':
	tradeDate = sys.argv[1]
	path = sys.argv[2]
	#dateTime = datetime.now().strftime("%Y%M%d_%H%M")	
	spark = SparkSession.builder.appName('powerPyspark').getOrCreate()
                    
	#sc = SparkContext("local", "Spark_Example_App")
	trades = trading.get_trades(tradeDate)
	print(trades)      
	df = spark.sparkContext.parallelize(trades).toDF()
	df1 = df.select(df.date,df.id,arrays_zip(df.time,df.volume))
	df2 = df1.select('id','date',explode('arrays_zip(time, volume)'))
	df3 =	df2.select('id','date',df2.col.time.alias('time'),df2.col.volume.alias('volume'))
	df4 = df3.withColumn('date_',to_date(col("date"),"dd/MM/yyyy"))
	df5 = df4.withColumn('time',when(df4.time=='NaN',None).otherwise(df4.time))
	data_stats = df5.describe()
	data_stats.show()
	data_stats.write.option('header',True).csv(path+'/'+'powerservices_'+dateTime+'_data_quality')
	df6 = df5.withColumn('volume',when(~df5.volume.isNotNull(),0).otherwise(df5.volume)).na.drop()
	df7 = df6.withColumn('hour',substring(df5.time,1,2))
	df8 = df7.groupBy('hour').avg('volume').orderBy('hour').select('hour',floor('avg(volume)').alias('volume'))
	df8 = df8.withColumn('hour',concat(df8.hour,lit(":00")))
	df9 = spark.createDataFrame(df8.tail(1)).union(spark.createDataFrame(df8.head(23)))

	df9.show(25,False)
	df9.write.option('header',True).csv(path+'/'+'powerservices_'+dateTime)
