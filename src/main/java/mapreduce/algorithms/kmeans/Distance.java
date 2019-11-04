package mapreduce.algorithms.kmeans;

public class Distance {

    public static double EuclidieanDistance(double p1_X, double p2_X, double p1_Y, double p2_Y){
        double deltaX = Math.abs(p1_X - p2_X);
        double deltaY = Math.abs(p1_Y - p2_Y);

        return Math.sqrt((deltaX * deltaX) + (deltaY * deltaY));
    }

    public static double getMagnitude(double x,double y) {
        return Math.sqrt((x * x) + (y * y));
    }
}
