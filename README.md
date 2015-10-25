# spark-samples

Various Samples using Spark computing from basic to advance.

Here are the steps to set-up your env and execute each sample:

1. git clone https://github.com/mangeet/spark-samples.git
2. Copy "videos_by_actor.csv" file from this project to your user home (user.home)
   This csv file contains near ~40K of records and good to use to test basic examples
3. Package this project. (Uber jar)
   mvn package
4. Go to your Spark home and execute:

   ./bin/spark-submit --class me.play.spark.<ClassName> <your-workspace>/spark-samples/target/spark-samples-0.0.1-SNAPSHOT.jar

   This script will ship programme to cluster(local in above script) and compute the results by executing tasks for each RDD partition.