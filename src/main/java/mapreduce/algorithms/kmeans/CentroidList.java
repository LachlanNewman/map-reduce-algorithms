package mapreduce.algorithms.kmeans;

import java.util.ArrayList;

public class CentroidList {

    private ArrayList<Centroid> centroidList;

    public CentroidList(){
        centroidList = new ArrayList<Centroid>();
    }

    public void add(Centroid centroid){
        centroidList.add(centroid);
    }

    public ArrayList<Centroid> getCentroidList(){
        return centroidList;
    }

    public Centroid getClosest(Point point){
        double minDistance = Double.MAX_VALUE;

        Centroid closestCentroid = centroidList.get(0);

        for (Centroid centroid: centroidList){
            double centroidX = centroid.getX().get();
            double centroidY = centroid.getY().get();
            double pointX = point.getX().get();
            double pointY = point.getY().get();

            double distance = Distance.EuclidieanDistance(centroidX,pointX,centroidY,pointY);

            if(distance < minDistance){
                minDistance = distance;
                closestCentroid = centroid;
            }
        }
        return closestCentroid;
    }
}
