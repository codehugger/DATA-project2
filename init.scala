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

/** This function calculates the Euclidean distance between two n-dimensional vectors
 * @param v1 vector one
 * @param v2 vector two
 *
 * @return Int distance between the two vectors
 */
def distance(a: SiftDescriptorContainer,  b: SiftDescriptorContainer) : Int = {
	var ret = 0;
	var i = 0;
	while ( i < a.vector.length ) {
		val t = a.vector(i) - b.vector(i)
		ret += t*t;
		i = i+1;
	}
	return ret;
}

def centroid(id: Int, sifts: Iterable[SiftDescriptorContainer]) : SiftDescriptorContainer = {
  // Create a new sift descriptor container to represent the new centroid
  var a: Array[Int] = new Array[Int](128)

  sifts.foreach(sift => {
    var i = 0
    sift.vector.foreach(x => {
      a(0) += x
    })
  })

  val centroid : SiftDescriptorContainer = new SiftDescriptorContainer()
  centroid.id = id

  var siftCount = sifts.size
  centroid.vector = a.map(x => (x/siftCount).toByte)
  
  centroid
}

// file locations
val seq_10k = "./SIFT/bigann_query.seq"
val seq_10m = "./SIFT/10M_samplefile.seq"

// load datasets
val rdd_10m = loadSIFTs(sc, seq_10m)
val rdd_10k = loadSIFTs(sc, seq_10k)

// choose k random vectors of 128-dimension
val K = 5

// K-means algorithm terminates when the change in centroid location is smaller than 0.1
val convDist = 0.1
var currDist = Double.PositiveInfinity
