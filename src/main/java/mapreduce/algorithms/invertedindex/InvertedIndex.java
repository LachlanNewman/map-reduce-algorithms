package mapreduce.algorithms.invertedindex;

import mapreduce.algorithms.bfs.Dijkstra;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<LongWritable,Text,Text, Posting> {

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] documentPostings = value.toString().split("=>");
            String documentId = documentPostings[0];
            String[] postings = documentPostings[1].split(" ");

            HashMap<String,Integer> postingsMap = new HashMap<String, Integer>();
            for (String posting: postings){
                if (postingsMap.containsKey(posting)){
                    int count = postingsMap.get(posting);
                    count++;
                    postingsMap.put(posting,count);
                }
                else {
                    postingsMap.put(posting,1);
                }
            }

            Set<String> postingMapKeys = postingsMap.keySet();

            for (String postingMapKey: postingMapKeys){
                Posting posting = new Posting(documentId,postingsMap.get(postingMapKey));
                context.write(new Text(postingMapKey),posting);
            }

        }
    }

    public static class InvertedIndexReducer extends Reducer<Text,Posting,Text,Text> {

        public void reduce(Text key,Iterable<Posting> values,Context context) throws IOException, InterruptedException {
            String postingList = "";
            for (Posting post:values){
                postingList = postingList + post.toString();
            }
            context.write(key,new Text(postingList));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String input = "invertedindex.txt";
        String output = "outinvertedindex";

        String jobName = "BFS";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Posting.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
