package mapreduce.algorithms.bfs;

import java.util.ArrayList;

public class Graph {

    private ArrayList<Node> nodes;

    public Graph(){
        nodes = new ArrayList<Node>();
    }

    public void addNode(Node node){
        nodes.add(node);
    }
}
