package one.lastime

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.to_timestamp
import java.time.LocalDateTime

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}

import util.control.Breaks._
import com.databricks.spark.xml._
import java.time.format.DateTimeFormatter

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration



object xmlTransformer {


  def fileTransformation(file: String,spark :SparkSession,table_path:String,hdfs_location:String) = {


    println (file)
    // your logic of processing a single file comes here


    val df = spark
      .read
      .format("com.databricks.spark.xml")
      .option("rowTag", "T").load(file)

    val regex = "\\d{4}-\\d{2}-\\d{2}-\\d{2}:\\d{2}".r

    val file_stamp = regex.findFirstIn(file).getOrElse("no match")
    file_stamp.replaceAll(":","")
    print(file_stamp+" super")
    breakable {
      if (file_stamp == "no match") {
        break
      }

    }



    val df1=df.filter(df("C_ACCTBAL")>0)




    val file_path="hdfs://"+hdfs_location+"/customer"+file_stamp+".parquet"

    df1.write.parquet(file_path)
    val hadoopconf = new Configuration();
    val fs = FileSystem.get(hadoopconf);
    spark.sql("alter table ica.customer add partition (file_stamp='%s')".format(file_stamp))
    //Create output stream to HDFS file
    val outFileStream = fs.create(new Path("hdfs://"+table_path+"/file_stamp=%s/".format(file_stamp)))

    //Create input stream from local hdfs file
    val inStream = fs.open(new Path(file_path))

    IOUtils.copy(inStream, outFileStream)

    //Close both files
    inStream.close()
    outFileStream.close()






  }
  def main(args: Array[String]): Unit = {


    //Enabling Hive for SparkSession
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .enableHiveSupport() .getOrCreate()

    val hdfs_file_directory= args(0)
    val table_path=args(1)
    val hdfs_location=args(2)

    //Get all the files in the hdfs source directory
    val files = FileSystem.get( spark.sparkContext.hadoopConfiguration ).listStatus(new Path(hdfs_file_directory))

    //Perform XML Conversion logic
    files.foreach( filename => {
      // the following code makes sure "_SUCCESS" file name is not processed
      val a = filename.getPath.toString()
      val m = a.split("/")
      val name = m(10)
      println("\nFILENAME: " + name)

      fileTransformation(a,spark,table_path,hdfs_location)


    })






  }

}
