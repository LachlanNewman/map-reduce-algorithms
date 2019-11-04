package mapreduce.algorithms.bfs;

import java.io.*;
import java.util.LinkedHashMap;

public class AdjacencyList {

    private static LinkedHashMap<Integer, Node> adjacencyList;

    public static String[] getNodes(String nodes){
        return nodes.split("'");
    }

    public static void generateAdjacencyList(String startNode) {
        try {

            String fileName = "bfs_input";
            String graphFile = "graph.txt";

            //File holding the input adjacency matrix
            File input = new File(fileName);
            //File holding the graph reformatted with infinite values
            File graph = new File(graphFile);

            FileWriter fw = new FileWriter(graph);
            FileReader fr = new FileReader(input);


            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                String[] nodeList = line.split(":");
                String nodeName = nodeList[0];
                String graphList = nodeName + ":";

                fw.write(graphList);

                if(startNode.equals(nodeName)){
                    writeKnowDistances(fw, nodeList[1].split(","));
                }
                else {
                    writeInfiniteNodes(fw, nodeList[1].split(","));
                }

                fw.write("\n");

            }
            fw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeKnowDistances(FileWriter fw, String[] nodes) throws IOException {
        for (String node : nodes) {
            fw.write(node + ",");
        }
    }

    private static void writeInfiniteNodes(FileWriter fw, String[] nodes) throws IOException {
        for (String node : nodes) {
            String nodeName = String.valueOf(node.charAt(0));
            String infinte = String.valueOf(Long.MAX_VALUE);
            fw.write(nodeName+infinte + ",");
        }
    }

    public static void main(String[] args) {
        generateAdjacencyList("A");
    }
}
