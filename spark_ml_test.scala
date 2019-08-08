#Logistic regression with elastic net penalty
import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.linalg.{Vector, Vectors}

import org.apache.spark.ml.param.ParamMap

import org.apache.spark.sql.Row
import java.util.Date
val start_time = new Date().getTime
val wine = spark.read.option("header","true").option("inferSchema","true").csv("E:/wine.csv")
val training_rdd = wine.map(r=>(r.get(0).toString().toDouble,Vectors.dense(r.get(1).toString().toDouble,r.get(2).toString().toDouble,r.get(3).toString().toDouble,r.get(4).toString().toDouble,r.get(5).toString().toDouble,r.get(6).toString().toDouble,r.get(7).toString().toDouble,r.get(8).toString().toDouble,r.get(9).toString().toDouble,r.get(10).toString().toDouble,r.get(11).toString().toDouble,r.get(12).toString().toDouble,r.get(13).toString().toDouble)))

val training = training_rdd.toDF("label","features")
val lr = new LogisticRegression()
lr.setMaxIter(10).setRegParam(0.01).setElasticNetParam(0.5)
val model1 = lr.fit(training)
val end_time = new Date().getTime
println(end_time-start_time)

spark-shell --driver-memory 10G --executor-memory 15G --executor-cores 8
#Logistic Regression without penalty
import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.linalg.{Vector, Vectors}

import org.apache.spark.ml.param.ParamMap

import org.apache.spark.sql.Row
import java.util.Date
val start_time = new Date().getTime
val wine = spark.read.option("header","true").option("inferSchema","true").csv("E:/data_train.csv")
val training_rdd = wine.map(r=>(r.get(0).toString().toDouble,Vectors.dense(r.get(1).toString().toDouble,r.get(2).toString().toDouble,r.get(3).toString().toDouble,r.get(4).toString().toDouble,r.get(5).toString().toDouble,r.get(6).toString().toDouble,r.get(7).toString().toDouble,r.get(8).toString().toDouble,r.get(9).toString().toDouble,r.get(10).toString().toDouble,r.get(11).toString().toDouble,r.get(12).toString().toDouble,r.get(13).toString().toDouble)))

val training = training_rdd.toDF("label","features")
val lr = new LogisticRegression()
lr.setMaxIter(50)
val model1 = lr.fit(training)
val end_time = new Date().getTime
println(end_time-start_time)

#Linear SVM
import org.apache.spark.ml.classification.LinearSVC
val start_time = new Date().getTime
val wine = spark.read.option("header","true").option("inferSchema","true").csv("E:/wine.csv")
val training_rdd = wine.map(r=>(r.get(0).toString().toDouble,Vectors.dense(r.get(1).toString().toDouble,r.get(2).toString().toDouble,r.get(3).toString().toDouble,r.get(4).toString().toDouble,r.get(5).toString().toDouble,r.get(6).toString().toDouble,r.get(7).toString().toDouble,r.get(8).toString().toDouble,r.get(9).toString().toDouble,r.get(10).toString().toDouble,r.get(11).toString().toDouble,r.get(12).toString().toDouble,r.get(13).toString().toDouble)))

val training = training_rdd.toDF("label","features")
val lsvm = new LinearSVC()
lsvm.setMaxIter(10)
val model1 = lsvm.fit(training)
val end_time = new Date().getTime
println(end_time-start_time)
