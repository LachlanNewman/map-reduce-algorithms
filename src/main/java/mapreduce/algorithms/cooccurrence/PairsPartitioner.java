package mapreduce.algorithms.cooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Map;

public class PairsPartitioner extends Partitioner<WordPair, IntWritable> {

    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
        Map<Character, Integer> char2Integer = PartitionConverter.getCharacterIntegerHashMap();

        int partitionNumber = 0;

        try {
            char firstLetter = wordPair.getWord().toString().toLowerCase().charAt(0);


            if (char2Integer.containsKey(firstLetter)) {
                partitionNumber = char2Integer.get(firstLetter) % numPartitions;
            }

        }
        catch (StringIndexOutOfBoundsException e){
            System.out.println(e);
        }
        return partitionNumber;
    }
}
