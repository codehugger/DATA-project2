import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.io.IntWritable
import eCP.Java.SiftDescriptorContainer

while (tempDist > convDist) {
  val bc_centroids = sc.broadcast(centroids)

  val distRDD = rdd_10k.map(desc => {
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
  val groupedRDD = distRDD.groupByKey().sortBy(_._1)
  
  // calculate new centroid for each key (any better ideas for a
  // better id on the centroid than to keep the initial id are welcome)
  val newCentroids = groupedRDD.map(x => (x._1, calcCentroid(x._1, x._2))).map(x => x._2).collect.sortBy(_.id)

  bc_centroids.unpersist(true)
  val bc_centroids = sc.broadcast(newCentroids)
  
  // calculate the distance to see if we should stop based on convDist
  // FIXME: this does not work
  val tempDist = newCentroids.zip(centroids).map(x => distance(x._1, x._2)).sum
  println(tempDist)
}