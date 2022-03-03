import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.io.IntWritable
import eCP.Java.SiftDescriptorContainer

// 0. init K number of centroids from collection (use seed: 42 to keep it consistent)
var centroids = rdd_10k.takeSample(false, K, 42)

while (tempDist > convDist) {
  val distRDD = rdd_10k.map(desc => {
    val dist_id_pair = centroids.map(c => (distance(c, desc), c.id));
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
  val groupedRDD = distRDD.groupByKey()
  
  // calculate new centroid for each key (any better ideas for a
  // better id on the centroid than to keep the initial are welcome)
  val newCentroids = groupedRDD.map(x => (x._1, centroid(x._1, x._2)))

  // calculate the distance to see if we should stop based on convDist
  // ...
}