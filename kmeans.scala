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

def calcCentroid(id: Int, sifts: Iterable[SiftDescriptorContainer]) : SiftDescriptorContainer = {
  // Create a container for the new centroid vector
  var a: Array[Int] = new Array[Int](128)

  sifts.foreach(sift => {
    var i = 0
    sift.vector.foreach(x => {
      a(i) += x
      i += 1
    })
  })

  // Create the new SIFT vector to hold the centroid
  val centroid : SiftDescriptorContainer = new SiftDescriptorContainer()
  centroid.id = id

  // find the average for each dimension in the vector
  var siftCount = sifts.size
  centroid.vector = a.map(x => (x/siftCount).toByte)
  
  centroid
}

def kmeans(sifts : RDD[SiftDescriptorContainer], k : Int = 100, maxIter : Int = 50, convDist : Double = 0.1) : Unit = {
  // K-means algorithm terminates when the change in centroid location is smaller than 0.1
  val convDist = 0.1

  // Init K number of centroids from collection (use seed: 42 to keep it consistent)
  println(s"Sampling $k centroids from SIFTs collection")
  var centroids = sifts.takeSample(false, k, 42).sortBy(_.id)
  println(s"Broadcasting centroids")
  var bc_centroids = sc.broadcast(centroids)

  // Initialize the stoppers
  var currDist = Double.PositiveInfinity
  var counter = 0

  while (counter <= maxIter && convDist <= currDist) {
    println("------------------------------------------------------------")
    println(s"- Running K-means iteration $counter")

    val distanceRDD = sifts.map(desc => {
      val dist_id_pair = bc_centroids.value.map(c => (distance(c, desc), c.id));
      val dist_id_pair_sorted = dist_id_pair.sortBy(_._1)

      // return value .. what will the new RDD contain?
        // the <Dist, NN-ID> pair ?
      //dist_id_pair_sorted(0) 
        // OR  <Dist, NN-ID, SIFT> tuple?
      //(dist_id_pair_sorted(0), desc);
        // OR <NN-ID, SIFT> pair ? 
      //(dist_id_pair(0)._2, desc)
        // or if we just want to count "distribution" we can do
      (dist_id_pair_sorted(0)._2, desc)
    })
    
    // collapse on the id of the centroid
    val groupedRDD = distanceRDD.groupByKey()
    
    // calculate new centroid for each key (any better ideas for a
    // better id on the centroid than to keep the initial id are welcome)
    val newCentroids = groupedRDD.map(x => (x._1, calcCentroid(x._1, x._2)))
                                .map(x => x._2)
                                .collect
                                .sortBy(_.id)
        
    // calculate the distance to see if we should stop based on convDist
    // FIXME: this does not work ... I think
    currDist = newCentroids.zip(bc_centroids.value).map(x => distance(x._1, x._2)).sum

    println("- Unpersisting centroids")
    bc_centroids.unpersist(true)

    println("- Broadcasting recalculated centroids")
    bc_centroids = sc.broadcast(newCentroids)
    // centroids = newCentroids

    println(s"- Current distance $currDist")
    println(s"- End of iteration $counter")

    counter += 1
  }
}

// file locations
val seq_10k = "./SIFT/bigann_query.seq"
val seq_10m = "./SIFT/10M_samplefile.seq"

// load datasets
val rdd_10m = loadSIFTs(sc, seq_10m)
val rdd_10k = loadSIFTs(sc, seq_10k)
