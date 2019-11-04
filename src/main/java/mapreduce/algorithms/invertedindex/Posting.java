package mapreduce.algorithms.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Posting implements Writable, WritableComparable<Posting> {

    private Text documentId;
    private IntWritable frequency;

    public Text getDocumentId() {
        return documentId;
    }

    public void setDocumentId(Text documentId) {
        this.documentId = documentId;
    }

    public Posting(String documentId, int frequency) {
        this.documentId = new Text(documentId);
        this.frequency = new IntWritable(frequency);
    }

    public Posting(Text documentId, IntWritable frequency) {
        this.documentId = documentId;
        this.frequency = frequency;
    }

    public Posting(){
        this.documentId = new Text();
        this.frequency = new IntWritable();
    }

    public int compareTo(Posting o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.documentId.write(dataOutput);
        this.frequency.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.documentId.readFields(dataInput);
        this.frequency.readFields(dataInput);
    }

    public String toString(){
        return "[" + documentId.toString() + ":" + frequency.toString() + "]";
    }
}
