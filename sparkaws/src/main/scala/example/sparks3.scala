package example

import org.apache.spark.sql.SparkSession

object SparkS3 {
    def main(args:Array[String]) {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[3]")
            .appName("Spark AWS S3")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val sc = spark.sparkContext

        val accessKey = "ACCESS_KEY"
        val secretAccessKey = "SECRET_ACCESS_KEY"
        val bucket = "BUCKET_NAME"

        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)
        // spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
        spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        // // Read CSV file into DataFrame.
        // println("\nRead csv file into a DF...")
        // val df1 = spark.read.csv(
        // "file:///Users/AjayJayantilalSingal/Big Data/SparkSamples/resources/zipcodes.csv"
        // )
        // df1.printSchema()

        // // Write the file to S3.
        // println("Writing to S3...")
        // df1.write.format("csv")
        //     .mode("Overwrite")
        //     .save(s"s3a://${bucket}/zipcodes")

        // println("Reading from S3...")
        // val file = s"s3a://${bucket}/zipcodes/part*.csv"
        // val lines = sc.textFile(file)
        // val lineCount = lines.count()
        // println(lineCount)
        
        println("Create the DF...")
        val data = Seq(("James ","","Smith","36636","M",3000),
            ("Michael ","Rose","","40288","M",4000),
            ("Robert ","","Williams","42114","M",4000),
            ("Maria ","Anne","Jones","39192","F",4000),
            ("Jen","Mary","Brown","","F",-1)
            )

        val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
        import spark.sqlContext.implicits._
        val df = data.toDF(columns:_*)
        df.printSchema()
        df.show()

        // println("Write DF to S3 in parquet format...")
        // df.write.mode("overwrite").parquet(s"s3a://${bucket}/people.parquet")    // Folder.
        println("Write DF to S3 in csv format...")
        df.write.mode("overwrite").csv(s"s3a://${bucket}/people.csv")       // Folder.

        // println("Read parquet file from S3 into DF...")
        // val parqDF = spark.read.parquet(s"s3a://${bucket}/people.parquet")
        println("Read csv file from S3 into DF...")
        val parqDF = spark.read.csv(s"s3a://${bucket}/people.csv")
        parqDF.printSchema()
        parqDF.show()


        println("##spark read text files from a directory into RDD")
        val rddFromFile = spark.sparkContext.textFile(s"s3a://${bucket}/people.csv/*.csv")
        println(rddFromFile.getClass)

        println("##Get data Using collect")
        rddFromFile.collect().foreach(f=>{
            println(f)
        })        
    }
}
