package mapreduce.algorithms.kmeans;

import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Centroid,Point> {

    public int getPartition(Centroid centroid, Point point, int numReduceTasks) {

        int clusterIndex = centroid.getClusterIndex().get();
        return clusterIndex;
    }
}
