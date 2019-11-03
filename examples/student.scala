import org.apache.spark.sql._
import org.apache.spark.sql.types._


object app {

     def main(args: Array[String]) {
     
        val schema = StructType(
            List(StructField("id", IntegerType, true),
                 StructField("name", StringType,true),
                 StructField("age", IntegerType, true),
                 StructField("subject", StringType, true),
                 StructField("grade", IntegerType, false) 
             ))
        
        // 查询json格式文件
        val jsondf = spark.read
                .format("minioSelectJSON")
                .schema(schema)
                .load("cos://bucketname-123456789/student.json")
        jsondf.select("id", "name", "subject", "grade").filter("grade > 80").show()
        
        // 查询csv格式文件
        val csvdf = spark.read
                .format("minioSelectCSV")
                .schema(schema)
                .load("cos://bucketname-123456789/student.csv")
        csvdf.select("*").filter("age > 11").show()
        
     }
}
