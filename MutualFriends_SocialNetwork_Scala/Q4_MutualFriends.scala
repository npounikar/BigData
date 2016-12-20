import java.util.Date
import org.apache.spark.sql.SQLContext

val currDate = new Date(System.currentTimeMillis)
def getAge(dob: String) ={
    val date=dob.split("/")
     val Month = currDate.getMonth()+1
     val Year = currDate.getYear()+1900
     var age = Year - date(2).toInt
     if(date(0).toInt>Month)
		age-= 1
     else if(date(0).toInt==Month){
		val currentDay=currDate.getDate();
		if(date(1).toInt>currentDay) 
			age-= 1
     }
     age.toFloat
}


val inputUserIds = sc.textFile("hdfs://cshadoop1/socNetData/networkdata")
val friendsMappedToUser = inputUserIds.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).map(li=>(li(0),li(1).split(","))).flatMap(x=>x._2.flatMap(z=>Array((z,x._1))))
val inputUserData = sc.textFile("hdfs://cshadoop1/socNetData/userdata")
val data1 = inputUserData.map(li=>li.split(",")).map(li=>(li(0),getAge(li(9))))
val data2 = friendsMappedToUser.join(data1)
val maxAge = data2.groupBy(_._2._1).mapValues(x=>x.maxBy(_._2._2))
val sortedAgeDesc = maxAge.sortBy(-_._2._2._2)
val output = sortedAgeDesc.take(10)
val outputSC = sc.parallelize(output)
val data3 = inputUserData.map(li=>li.split(",")).map(li=>(li(0),li(1),li(3),li(4),li(5)))
val key = data3.map({case(first,second,third,fourth,fifth) => first->(second,third,fourth,fifth)})
val joinVar = outputSC.join(key).sortBy(_._2,false)
val output = joinVar.map(x=>(x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4,x._2._1)).collect.mkString("\n").replace("(","").replace(")","")
System.exit(0)
