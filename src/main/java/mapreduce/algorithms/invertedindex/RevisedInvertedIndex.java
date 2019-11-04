package mapreduce.algorithms.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class RevisedInvertedIndex {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] documentPostings = value.toString().split("=>");
            String documentId = documentPostings[0];
            String[] postings = documentPostings[1].split(" ");

            HashMap<String, Integer> postingsMap = new HashMap<String, Integer>();
            for (String posting : postings) {
                if (postingsMap.containsKey(posting)) {
                    int count = postingsMap.get(posting);
                    count++;
                    postingsMap.put(posting, count);
                } else {
                    postingsMap.put(posting, 1);
                }
            }

            Set<String> postingMapKeys = postingsMap.keySet();

            for (String postingMapKey : postingMapKeys) {
                context.write(new Tuple(postingMapKey,documentId),new IntWritable(postingsMap.get(postingMapKey)));
            }

        }
    }

    public static class InvertedIndexReducer extends Reducer<Tuple, IntWritable, Text, Text> {

        String tPrev = null;
        String postingList = "";

        public void reduce(Tuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String term  = key.getTerm().toString();

            if (!term.equals(tPrev) && tPrev != null){
                context.write(key.getTerm(),new Text(postingList));
                postingList = "";
            }


            for (IntWritable frequency : values) {
                Posting post = new Posting(key.getDocumentId(),frequency);
                postingList = postingList + post.toString();
            }
            System.out.println(term.toString());
            tPrev = term;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(tPrev),new Text(postingList));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String input = "invertedindex.txt";
        String output = "outinvertedindex";

        String jobName = "BFS";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(RevisedInvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Tuple.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
