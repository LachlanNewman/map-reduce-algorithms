package mapreduce.algorithms.kmeans;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable, WritableComparable<Point> {

    private DoubleWritable x;

    public void setX(DoubleWritable x) {
        this.x = x;
    }

    public void setY(DoubleWritable y) {
        this.y = y;
    }

    public DoubleWritable getX() {
        return x;
    }

    public DoubleWritable getY() {
        return y;
    }

    private DoubleWritable y;

    public Point(DoubleWritable x, DoubleWritable y) {
        this.x = x;
        this.y = y;
    }

    public Point(int x, int y) {
        this.x = new DoubleWritable(x);
        this.y = new DoubleWritable(y);
    }

    public Point() {
        this.x = new DoubleWritable();
        this.y = new DoubleWritable();
    }


    public int compareTo(Point o) {
        double magnitude = Distance.getMagnitude(x.get(),y.get());
        double oMagnitude = Distance.getMagnitude(o.getX().get(),o.getY().get());
        return Double.compare(magnitude, oMagnitude);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.x.write(dataOutput);
        this.y.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.x.readFields(dataInput);
        this.y.readFields(dataInput);
    }
}
