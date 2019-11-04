package mapreduce.algorithms.bfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Dijkstra {

    public static class DijkstraMapper extends Mapper<LongWritable,Text,LongWritable, Text> {

        public void map(LongWritable key,Text value,Context context){
            String[] adjacency_list_item = value.toString().split(":");
            String node = adjacency_list_item[0];
            String[] outlinks = adjacency_list_item[1].split(",");
            for (String outlink: outlinks){
                String name = String.valueOf(outlink.charAt(0));
            }

        }
    }

    public static class DijkstraReducer extends Reducer<LongWritable,Text,LongWritable, Text> {

        public void reduce(LongWritable key,Iterable<Text> values,Context context){

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String input = "bfs_input";
        String output = "out";

        String jobName = "BFS";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Dijkstra.class);
        job.setMapperClass(DijkstraMapper.class);
        job.setReducerClass(DijkstraReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
//        job.

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
