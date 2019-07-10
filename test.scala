import org.apache.spark.sql.types.DataTypes
import java.sql.Timestamp
import java.util.Date


case class tick(Time:Timestamp,secucode:Integer,price:Double,cumv:Integer,askp:Double,askv:Integer,bidp:Double,bidv:Integer)
val ds = spark.read.option("header","true").option("inferSchema","true").csv("E:\\tick.csv").as[tick]
val start_time = new Date().getTime
val ds_drop = ds.drop("_c0","secucode","cumv","askp","askv","bidp","bidv")

val new_df= ds_drop.select(col("*"),
     udf{
     (e:Int)=>{
     val time_str = (e-1000).toString
     if (time_str.length == 8){
         val time = time_str.slice(0,3).toInt+1
         val time_str_last = time_str.slice(1,3)
         if (time_str_last == "59"){
           (time+40).toInt
         }else{
           time
         }
     }else{
         val time = time_str.slice(0,4).toInt+1
         val time_str_last = time_str.slice(2,4)
         if (time_str_last == "59"){
           (time+40).toInt
         }else{
           time
         }
      }} 
      }.apply(ds_drop("time")).cast(DataTypes.IntegerType).as("minute_time"))

case class new_tick(Time:Integer,Price:Double,minute_time:Integer)
val new_ds = new_df.as[new_tick]

case class prices_all(minute_time:Integer, max_p:Double, min_p:Double, open_p:Double,close_p:Double)
val prices_all = new_ds.groupBy("minute_time").agg(max("price").as("max_p"),min("price").as("min_p"),first("price").as("open_p"),last("price").as("close_p"))

val end_time = new Date().getTime
println(start_time-end_time)
     
