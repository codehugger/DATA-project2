# DATA project 2

# Config for running big RDDs

To make sure Java doesn't run out of heap memory you need to change the settings in %SPARKDIR$/conf/spark-defaults.conf to

```
spark.driver.memory	16g
```

Please note that 16g is probably overkill compared to the default which is 5g but this hasn't been tested so we are going with the "too much is always enough" methodology for now.

# Preparing the SIFTs folder

There is a folder included called SIFT which has one file in it i.e. the 10K SIFT seq file. To run the code it needs to be populated with the 10M SIFT file (1.4GB) from skel.ru.is. Run the following to populate the folder:

```bash
> cd SIFT
> wget http://skel.ru.is/bdm/10M_samplefile.seq
```

# Running the kmeans

To run the spark-shell and load the code run the following

```bash
> spark-shell --jars SDC_only.jar -i kmeans.scala
```

This loads the necessary scala code into the spark-shell in order to be able to run

```bash
> kmeans(rdd_10k)
```

For the smaller set of SIFTs which should take around 4 sec on a modern machine.

```bash
> kmeans(rdd_10m)
```

For the larger set of SIFTs which takes about 30 minutes on a decent server (Tested on Ryzen 9 with 64GB RAM and 4TB fast SSD)
