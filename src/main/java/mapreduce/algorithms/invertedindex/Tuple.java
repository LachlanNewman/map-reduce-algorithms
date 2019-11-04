package mapreduce.algorithms.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple implements Writable, WritableComparable<Tuple> {

    private Text term;
    private Text documentId;

    public Text getTerm() {
        return term;
    }

    public void setTerm(Text term) {
        this.term = term;
    }

    public Text getDocumentId() {
        return documentId;
    }

    public void setDocumentId(Text documentId) {
        this.documentId = documentId;
    }

    public Tuple() {
        this.term = new Text();
        this.documentId = new Text();
    }

    public Tuple(String term, String documentId) {
        this.term = new Text(term);
        this.documentId = new Text(documentId);
    }

    public Tuple(Text term, Text documentId) {
        this.term = term;
        this.documentId = documentId;
    }

    public int compareTo(Tuple o) {
        int compare = this.term.toString().compareTo(o.getTerm().toString());
        if (compare == 0){
            compare = this.documentId.toString().compareTo(o.getDocumentId().toString());
        }
        return compare;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.documentId.write(dataOutput);
        this.term.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.documentId.readFields(dataInput);
        this.term.readFields(dataInput);
    }

    public String toString(){
        return "[" + term.toString() + ":" + documentId.toString() + "]";
    }
}
