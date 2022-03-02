import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.io.IntWritable
import eCP.Java.SiftDescriptorContainer

/** This function is specificly designed load a file of SIFT descriptonrs into an RDD. 
 * @param sc SparkContext of our master node. 
 * @param fileName Name and path to the file full of SIFT descriptors --  
 *        format: [hadoop.io.IntWriteable, eCP.Java.SiftDescriptorContainer].
 * @return RDD[eCP.Java.SiftDescriptorContainer] RDD of all the SIFT's in the file (using the key as the SIFT's ID). 
 */
def loadSIFTs(sc: SparkContext, fileName: String) : RDD[SiftDescriptorContainer] = {
  // We use the spark context to read the data from a HDFS sequence file, but we need to 
  // load the hadoop class for the key (IntWriteable) and map it to the ID
  sc.sequenceFile(fileName, classOf[IntWritable], classOf[SiftDescriptorContainer]).map(it => {
    // first we map to avoid HDFS reader "re-use-of-writable" issue.
    val desc : SiftDescriptorContainer = new SiftDescriptorContainer()
    desc.id = it._2.id
    // WARNING! --> DeepCopy needed as the Java HDFS reader re-uses the memory :(
    it._2.vector.copyToArray(desc.vector) 
    desc
  })
}