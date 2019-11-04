package mapreduce.algorithms.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class Kmeans {

    final static int NUM_CENTROIDS = 10;

    private static int interation = 0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //create sequence file for centroids
        Path centroidDataPath = new Path("clustering/centroids.seq");
        conf.set(Paths.CENTROID_CONF, centroidDataPath.toString());

        //create sequence file to data points
        Path dataPointsPath = new Path("clustering/datapoints.seq");
        conf.set(Paths.POINTS_CONF, dataPointsPath.toString());

        //Create output data path
        Path outputPath = new Path("interation" + interation + "/new_points");
        conf.set(Paths.OUTPUT_CONF, dataPointsPath.toString());

        //Delete Previous output and sequence files
        FileSystem fs = FileSystem.get(conf);
        deletePrevPaths(outputPath, centroidDataPath, dataPointsPath, fs);

        generateCentroids(conf, centroidDataPath, fs);
        generateDataPoints(conf, dataPointsPath, fs);
        readCentroids(fs,conf);

        Job job = Job.getInstance(conf);
        job.setJobName("KMeans Clustering");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
//        job.setPartitionerClass(KMeansPartitioner.class);
        job.setJarByClass(KMeansMapper.class);

        FileInputFormat.addInputPath(job, dataPointsPath);


        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Centroid.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputValueClass(Point.class);
        job.setMapOutputKeyClass(Centroid.class);


        job.waitForCompletion(true);

        System.out.println(KMeansReducer.converged);

        while (!KMeansReducer.converged){

            conf = new Configuration();

            //create sequence file for centroids
            centroidDataPath = new Path("interation" + interation + "/new_points/part-r-00000");
            conf.set(Paths.CENTROID_CONF, centroidDataPath.toString());

            interation++;

            //create sequence file to data points
            dataPointsPath = new Path("clustering/datapoints.seq");
            conf.set(Paths.POINTS_CONF, dataPointsPath.toString());

            //Create output data path
            String oldOutputPath = outputPath.toString();
            outputPath = new Path("interation" + interation + "/new_points");
            conf.set(Paths.OUTPUT_CONF, dataPointsPath.toString());

            job = Job.getInstance(conf);
            job.setJobName("KMeans Clustering");

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
//        job.setPartitionerClass(KMeansPartitioner.class);
            job.setJarByClass(KMeansMapper.class);

            FileInputFormat.addInputPath(job, dataPointsPath);


            job.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(Centroid.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapOutputValueClass(Point.class);
            job.setMapOutputKeyClass(Centroid.class);


            job.waitForCompletion(true);
            System.out.println(KMeansReducer.converged);
            readCentroids(fs,conf);

        }

    }

    private static void readCentroids(FileSystem fs, Configuration conf) {
        //read centroids from centroids.sql
        for (int i = 0; i<= interation; i++) {
            Path path = new Path("interation" + i + "/new_points/part-r-00000");
            SequenceFile.Reader reader = null;
            try {
                reader = new SequenceFile.Reader(fs, path, conf);
                Centroid key = new Centroid();
                while (reader.next(key)) {
                    System.out.println(key.toString());
                }
                System.out.println("==================================================");
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void generateDataPoints(Configuration conf, Path dataPointsPath, FileSystem fs) {
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dataPointsPath, Point.class, IntWritable.class);

            int numDataPoints = 20;
            Random random = new Random();

            for (int i = 0; i < numDataPoints; i++) {
                int x = random.nextInt(100);
                int y = random.nextInt(100);

                writer.append(new Point(x, y), new IntWritable(0));
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void generateCentroids(Configuration conf, Path centroidDataPath, FileSystem fs) {
        try {
            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, centroidDataPath, Centroid.class, IntWritable.class);

            Random random = new Random();

            for (int i = 0; i < NUM_CENTROIDS; i++) {
                int x = random.nextInt(100);
                int y = random.nextInt(100);

                writer.append(new Centroid(x, y, i) , new IntWritable(i));
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void deletePrevPaths(Path outputPath, Path centroidDataPath, Path dataPointsPath, FileSystem fs) {
        try {
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            if (fs.exists(centroidDataPath)) {
                fs.delete(centroidDataPath, true);
            }

            if (fs.exists(dataPointsPath)) {
                fs.delete(dataPointsPath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
