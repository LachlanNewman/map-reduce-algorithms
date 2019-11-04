package mapreduce.algorithms.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducer extends Reducer<Centroid, Point, Centroid, IntWritable> {


    static boolean converged;

    @Override
    protected void reduce(Centroid centroid, Iterable<Point> dataPoints, Context context) throws IOException, InterruptedException {

        Centroid oldCentroid = new Centroid(centroid);

        int count = 0;
        double x = 0;
        double y = 0;

        for (Point point : dataPoints) {
            x = x + point.getX().get();
            y = y + point.getY().get();
            count++;
        }

        centroid.updateCentroid(count,x,y);
        context.write(centroid, new IntWritable(0));

        if (!oldCentroid.equals(centroid)){
            converged = false;
        }
        else {
            converged = true;
        }
    }
}
