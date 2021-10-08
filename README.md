# stock-price-prediction

# Project : Stock Price Prediction using real time data
1. Use Kafka to fetch the stocks data in real time.
2. Move data to a centralised data store in the cloud for further analysis in the data pipeline.
3. Preprocess the data (Python + Spark).
4. Store the cleaned data on HDFS
5. Carry out the prediction using MLib
6. Visualization using flask

# Use Kafka to fetch the stocks data 

VERSIONS: 
hadoop 3.3.1
kafka_2.13-2.8.0 
pydoop 2.0.0 
Kafka-python-2.0.2

Step1: Start the zookeeper. 
sudo systemctl start zookeeper


Start Kafka. 
sudo systemctl start kafka 


Check the status if it is active. 
sudo systemctl status kafka


Step2: Create a producer.py and consumer.py file and write the code.


Step3: Install the necessary modules and now execute the consumer.py in the terminal using the following command. 
$ python3 consumer.py 
This will create stock_price_data directory in the hdfs and will also copy the empty stock_data.csv file into the stock_price_data directory.


Step4: Now run the producer.py file and then we will get the output in the consumer.py file and the program will start to write the data into the hdfs.
So now that we have our data,we can clean and process the data and create a dataframe and build a machine learning model 

# Clean , Preprocess and build ML model


Step5: Preprocessing the data to clean it
1.Create a python notebook and load create a spark session
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('Stock Data Processing').getOrCreate()


Step6: Load the data we fetched in spark dataframe
df=spark.read.csv("hdfs://localhost:9000/Sample/data.csv",inferSchema=True,header=True)


Step7: Clean the data
Removing the quotes

dataset2=dataset.withColumnRenamed('["time"','time')\
.withColumnRenamed(' "open"','open')\
.withColumnRenamed(' "high"','high')\
.withColumnRenamed(' "low"','low')\
.withColumnRenamed(' "close"','close')\
.withColumnRenamed(' "volume"]','volume')

Also, removing the unnecessary quotes and brackets
new_df = dataset2.withColumn('open', regexp_replace('open', '"', ''))\
.withColumn('time', regexp_replace('time', '\[', ''))\
.withColumn('time', regexp_replace('time', '"', ''))\
.withColumn('high', regexp_replace('high', '"', ''))\
.withColumn('low', regexp_replace('low', '"', ''))\
.withColumn('close', regexp_replace('close', '"', ''))\
.withColumn('volume', regexp_replace('volume', '\]', ''))\
.withColumn('volume', regexp_replace('volume', '"', ''))


Step8: Show the cleaned data


Step9: Create vectors from features using vector assembler.

featureassembler=VectorAssembler(inputCols=["open","high","low"],outputCol="Features")


Step10: Transform the data and sort in ascending order

output=featureassembler.transform(df2)
finalized_data=output.select("time","Features","close").sort("time",ascending=True)


Step11: Splitting the data into train and test data.
df10=finalized_data.withColumn("rank",percent_rank().over(Window.partitionBy().orderBy("time")))
train_df=df10.where("rank<=.8").drop("rank")
test_df=df10.where("rank>.8").drop("rank")


Step12: Write test data to parquet file for further use
test_df.write.parquet('testdata')


Step13: Create model with linear regression algorithm
regressor=LinearRegression(featuresCol='Features', labelCol='close')
lr_model=regressor.fit(train_df)
print(“Coefficients:” +str(lr_model.coefficients))
print(“Coefficients:” +str(lr_model.intercept))


Step14: Making predictions by transforming data into model
pred= lr_model.transform(test_df)
pred.select("Features","close","prediction").show(5)


Step5: Evaluating the model
RMSE: It is the square root of the mean of the square of all the errors ie. it is the standard deviation of the residuals(predicted errors).
from pyspark.ml.evaluation import RegressionEvaluator
regressionEvaluator = RegressionEvaluator(
predictionCol="prediction",
labelCol="close",
metricName="rmse")
rmse = regressionEvaluator.evaluate(predDF)
print(f"RMSE is {rmse:.1f}")


Step15: Save model 
lr_model.save(“stock_data_model”)



