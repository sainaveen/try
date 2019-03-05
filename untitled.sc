import org.apache.commons.io.FileSystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val hadoopConf = new Configuration()
val hdfs = FileSystem.get(hadoopConf)

hdfs.m

val srcPath = new Path("gs://programfile/customer.xml")
val destPath = new Path("gs://prgroamfile/customer.xml")

hdfs.copyFromLocalFile(srcPath, destPath)
hdfs.rename(srcPath,destPath)
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
val srcFileSystem: FileSystem = FileSystemUtil
  .apply(spark.sparkContext.hadoopConfiguration)
  .getFileSystem(srcPath)
val dstFileSystem: FileSystem = FileSystemUtil
  .apply(spark.sparkContext.hadoopConfiguration)
  .getFileSystem(destPath)

FileUtil.copy(srcPath,destPath)



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.io.IOUtils;



val hadoopconf = new Configuration();
val fs = FileSystem.get(hadoopconf);

//Create output stream to HDFS file
val outFileStream = fs.create(new Path("hdfs://user/hive/warehouse/ica.db/"))

//Create input stream from local file
val inStream = fs.open(new Path("hdfs://user/hive/warehouse/sample.txt"))

IOUtils.copy(inStream, outFileStream)

//Close both files
inStream.close()
outFileStream.close()