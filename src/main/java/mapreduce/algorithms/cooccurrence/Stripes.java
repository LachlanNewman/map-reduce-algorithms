package mapreduce.algorithms.cooccurrence;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class Stripes {


    public static class StripesMapperTextFile extends Mapper<LongWritable, Text, Text, MapWritable> {

        private DoubleWritable ONE = new DoubleWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int windowSize = context.getConfiguration().getInt("neighbors", 2);

            String[] words = value.toString().split(",|\\s+");

            for (int i = 0; i < words.length; i++) {
                if (words[i].isEmpty()) continue;

                String word = words[i];
                Text wordKey = new Text(word);
                MapWritable outMap = new MapWritable();

                int start = (i - windowSize < 0) ? 0 : i - windowSize;
                int end = (i + windowSize >= words.length) ? words.length - 1 : i + windowSize;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    if (words[j].isEmpty()) continue;

                    String neighbour = words[j];
                    Text neighbourKey = new Text(neighbour);
                    if (outMap.containsKey(neighbourKey)) {
                        DoubleWritable count = (DoubleWritable) outMap.get(neighbourKey);
                        count.set(count.get() + 1);
                        outMap.put(neighbourKey, count);
                    } else {
                        outMap.put(neighbourKey, ONE);
                    }

                }

                context.write(wordKey, outMap);
            }
        }
    }


    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

        private MapWritable incrementingMap = new MapWritable();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            incrementingMap.clear();
            for (MapWritable value : values) {

                int totalCount = addAll(value);
                computeRelativeFrequencies(totalCount);
            }
            context.write(key, incrementingMap);
        }

        private void computeRelativeFrequencies(double totalCount) {
            Set<Writable> keys = incrementingMap.keySet();
            for (Writable key: keys){
                try {
                    DoubleWritable fromCount = (DoubleWritable) incrementingMap.get(key);
                    DoubleWritable relativeCount = new DoubleWritable(fromCount.get()/totalCount);
                    incrementingMap.put(key,relativeCount);
                }
                catch (ClassCastException e){
                    e.printStackTrace();
                }

            }
        }

        private int addAll(MapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            int totalCount = 0;
            for (Writable key : keys) {
                DoubleWritable fromCount = (DoubleWritable) mapWritable.get(key);
                totalCount+= fromCount.get();
                if (incrementingMap.containsKey(key)) {
                    DoubleWritable count = (DoubleWritable) incrementingMap.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    incrementingMap.put(key, fromCount);
                }
            }
            return totalCount;
        }
    }

    private static void deletePreviousOutput(String outputPath, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.newInstance(configuration);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
    }

    public static void main(String[] args) throws Exception {

        final String TYPE = "Stripes";

        String inputPath = "big.txt";
        String outputPath = "stripesout";
        String reducers = "3";


        String jobName = TYPE + " Reducers " + reducers;

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Stripes.class);
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setPartitionerClass(StripesPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(reducers));
        deletePreviousOutput(outputPath, conf);
        job.setMapperClass(StripesMapperTextFile.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}