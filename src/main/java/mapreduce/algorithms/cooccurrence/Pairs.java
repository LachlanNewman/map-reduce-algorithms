package mapreduce.algorithms.cooccurrence;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class Pairs {
    public static class PairsMapperIMCB extends Mapper<LongWritable, Text, WordPair, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int windowSize = context.getConfiguration().getInt("neighbors", 2);

            String[] words = value.toString().split("\\s+");


            for (int i = 0; i < words.length; i++) {

                if (words[i].isEmpty()) continue;
                WordPair maginalCount = new WordPair(words[i], "*");
                int start = (i - windowSize < 0) ? 0 : i - windowSize;
                int end = (i + windowSize >= words.length) ? words.length - 1 : i + windowSize;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    if (words[j].isEmpty()) continue;
                    WordPair wordPair = new WordPair(words[i], words[j]);
                    context.write(wordPair, ONE);
                    context.write(maginalCount, ONE);
                }

            }


        }

    }


    public static class IntSumReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static void deletePreviousOutput(String outputPath, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.newInstance(configuration);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
    }

    public static void main(String[] args) throws Exception {

        final String TYPE = "Pairs";

        String inputPath = "big.txt";
        String outputPath = "pairsoutnew";
        String reducers = "3";
        boolean inMapCombining = false;

        String jobName = TYPE + " Reducers " + reducers;

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(Pairs.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(PairsPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(reducers));
        job.setMapperClass(PairsMapperIMCB.class);

        deletePreviousOutput(outputPath, conf);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
