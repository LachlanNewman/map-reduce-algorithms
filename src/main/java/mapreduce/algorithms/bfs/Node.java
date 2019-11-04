package mapreduce.algorithms.bfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Node{

    private Text name;
    private LongWritable value;

    public Node(String name, long value) {
        this.name = new Text(name);
        this.value = new LongWritable(value);
    }

    public Node(String name) {
        this.name = new Text(name);
        this.value = new LongWritable(Long.MAX_VALUE);
    }

    public Node(Text name, LongWritable value) {
        this.name = name;
        this.value = value;
    }

    public Node(Text name) {
        this.name = name;
        this.value = new LongWritable(Long.MAX_VALUE);
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public LongWritable getValue() {
        return value;
    }

    public void setValue(LongWritable value) {
        this.value = value;
    }

    public int compareTo(Node o) {
        return this.value.compareTo(o.getValue());
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.name.write(dataOutput);
        this.name.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.name.readFields(dataInput);
        this.value.readFields(dataInput);
    }

    @Override
    public String toString(){
        return "name=[" + this.name + "],value=[" + this.value + "]";
    }

}
