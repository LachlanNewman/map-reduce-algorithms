package mapreduce.algorithms.kmeans;

import mapreduce.algorithms.invertedindex.Posting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class KMeansMapper extends Mapper<Point, IntWritable, Centroid, Point> {

    private CentroidList centroidList;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //read centroids from centroids.sql
        Configuration conf = context.getConfiguration();
        Path centroids = new Path(conf.get(Paths.CENTROID_CONF));
        FileSystem fs = FileSystem.get(conf);

        centroidList = new CentroidList();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);
        Centroid key = new Centroid();
        while (reader.next(key)) {
            centroidList.add(new Centroid(key));
        }
        reader.close();
    }

    protected void map(Point point, IntWritable value, Context context) throws IOException, InterruptedException {
        Centroid centroid = centroidList.getClosest(point);
        context.write(centroid, point);
    }
}
