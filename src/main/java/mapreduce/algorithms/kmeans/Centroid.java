package mapreduce.algorithms.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid> {

    private DoubleWritable x;
    private DoubleWritable y;
    private IntWritable clusterIndex;

    public Centroid(DoubleWritable x, DoubleWritable y, IntWritable clusterIndex) {
        this.x = x;
        this.y = y;
        this.clusterIndex = clusterIndex;
    }

    public Centroid(int x, int y, int clusterIndex) {
        this.x = new DoubleWritable(x);
        this.y = new DoubleWritable(y);
        this.clusterIndex = new IntWritable(clusterIndex);
    }

    public Centroid(double x, double y, int clusterIndex) {
        this.x = new DoubleWritable(x);
        this.y = new DoubleWritable(y);
        this.clusterIndex = new IntWritable(clusterIndex);
    }

    public Centroid() {
        this.x = new DoubleWritable();
        this.y = new DoubleWritable();
        this.clusterIndex = new IntWritable();
    }

    public Centroid(Centroid centroid){
        this.x = new DoubleWritable(centroid.getX().get());
        this.y = new DoubleWritable(centroid.getY().get());
        this.clusterIndex = new IntWritable(centroid.getClusterIndex().get());
    }

    public DoubleWritable getX() {
        return x;
    }

    public void setX(DoubleWritable x) {
        this.x = x;
    }

    public DoubleWritable getY() {
        return y;
    }

    public void setY(DoubleWritable y) {
        this.y = y;
    }

    public IntWritable getClusterIndex() {
        return clusterIndex;
    }

    public void setClusterIndex(IntWritable clusterIndex) {
        this.clusterIndex = clusterIndex;
    }

    public void updateCentroid(double count,double x, double y ) {

        x = x /count;
        y = y/count;

        this.setX(new DoubleWritable(x));
        this.setY(new DoubleWritable(y));
    }

    public int compareTo(Centroid o) {
        return this.getClusterIndex().compareTo(o.getClusterIndex());
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.clusterIndex.write(dataOutput);
        this.x.write(dataOutput);
        this.y.write(dataOutput);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.clusterIndex.readFields(dataInput);
        this.x.readFields(dataInput);
        this.y.readFields(dataInput);
    }

    public String toString(){
        return clusterIndex.toString() + ": {" + x.toString() + " , " + y.toString() + "}";
    }

    public boolean equals(Centroid centroid){
        boolean equals = true;
        if (this.x.get() != centroid.getX().get()){
            equals = false;
        }
        if (this.y.get() != centroid.getY().get()){
            equals = false;
        }
        if (this.clusterIndex.get() != centroid.getClusterIndex().get()){
            equals = false;
        }

        return equals;
    }
}
