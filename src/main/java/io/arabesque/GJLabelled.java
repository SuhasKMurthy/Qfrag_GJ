package io.arabesque;

import com.koloboke.collect.IntIterator;
import io.arabesque.graph.BasicMainGraphQuery;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.graph.Vertex;
import io.arabesque.search.steps.QueryGraph;
import io.arabesque.search.trees.SearchEmbedding;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.collection.IntArrayList;

import java.io.IOException;
import java.util.*;

public class GJLabelled extends AbstractGenericJoin{
    BasicMainGraphQuery queryGraph = null;
    UnsafeCSRGraphSearch dataGraph = null;

    public GJLabelled(QueryGraph queryGraph, UnsafeCSRGraphSearch dataGraph, ThreadOutput outputStream){
        super(outputStream);
        this.dataGraph = dataGraph;
        this.queryGraph = queryGraph.queryGraph;
    }

    private List<Integer> sortQueryVertices(){
        int numVertices = queryGraph.getNumberVertices();
        List<Integer> sortedQVertices = new ArrayList<>();
        long[] qSizes = new long[numVertices];
        for(int i = 0; i < numVertices; i++){
            int qLbl = queryGraph.getVertexLabel(i);
            long qSize = dataGraph.getNumberVerticesWithLabel(qLbl);
            qSizes[i] = qSize;
            sortedQVertices.add(i);
        }

        Collections.sort(sortedQVertices, Comparator.comparing(s -> qSizes[sortedQVertices.indexOf(s)]));
        return sortedQVertices;
    }

    public void search(){
        //long startTime, loopTime = 0;

        Deque<PrefixTuple> prefixes = new ArrayDeque<>();

        int numVertices = queryGraph.getNumberVertices();

        //List<Integer> sortedQVertices = sortQueryVertices();

        //int initQueryVertexLabel = queryGraph.getVertexLabel(sortedQVertices.get(0));
        int initQueryVertexLabel = queryGraph.getVertexLabel(0);
        IntArrayList initVertices =
                dataGraph.getVerticesWithLabel(initQueryVertexLabel);

        for(int i = 0; i < initVertices.size(); i++){
            int currVertex = initVertices.getUnchecked(i);
            PrefixTuple prefixTuple = new PrefixTuple();
            prefixTuple.addToMap(initQueryVertexLabel, currVertex);
            prefixes.addLast(prefixTuple);
        }

        Map<Integer, Set<Integer>> mapAll = new HashMap<>();

        for(int i = 0; i < numVertices; i++){
            int currQueryVertexLabel = queryGraph.getVertexLabel(i);
            IntArrayList vert = dataGraph.getVerticesWithLabel(currQueryVertexLabel);
            Set<Integer> currSet = new HashSet<>();
            for (int m = 0; m < vert.size(); m++) {
                currSet.add(vert.getUnchecked(m));
            }
            mapAll.put(currQueryVertexLabel, currSet);
        }

        IntIterator searchExtensionsIterator = dataGraph.createNeighborhoodSearchIterator();
        for(int i = 1; i < numVertices; i++) {
            int currQueryVertex = queryGraph.getVertex(i).getVertexId(); // sortedQVertices.get(i);
            int currQueryVertexLabel = queryGraph.getVertexLabel(currQueryVertex);

            int[] vertices = queryGraph.getVertexNeighbours(currQueryVertex).toIntArray();
            Set<Integer> currLabels = new HashSet<>();
            for (int k = 0; k < vertices.length; k++) {
                currLabels.add(queryGraph.getVertexLabel(vertices[k]));
            }

            Set<Integer> currSet = mapAll.get(currQueryVertexLabel);
            int size = prefixes.size();
            for (int j = 0; j < size; j++) {
                PrefixTuple curr = prefixes.poll();
                PriorityQueue<Set<Integer>> extensions =
                        new PriorityQueue<>((a, b) -> a.size() - b.size());

                for (Integer label : currLabels) {

                    Integer val = curr.map.get(label);

                    Set<Integer> newSet = null;
                    if (val == null) {
                        newSet = currSet;
                        //newSet = new HashSet<>(currSet);
                        //extensions.add(currSet);
                    } else {
                        newSet = new HashSet<>();

                        dataGraph.setIteratorForNeighborsWithLabel(val,
                                currQueryVertexLabel,
                                searchExtensionsIterator);
                        if (searchExtensionsIterator == null || !searchExtensionsIterator.hasNext()) {
                            extensions = null;
                            break;
                        }

                        while (searchExtensionsIterator.hasNext()) {
                            int extension = searchExtensionsIterator.nextInt();
                            newSet.add(extension);
                        }
                    }
                    extensions.add(newSet);
                }

                if (extensions == null)
                    continue;
                Set<Integer> next = getIntersection(extensions);

                for (Integer n : next) {
                    PrefixTuple newTuple = new PrefixTuple();
                    newTuple.map = new HashMap<>(curr.map);
                    newTuple.addToMap(currQueryVertexLabel, n);
                    prefixes.addLast(newTuple);
                }
            }
        }
//        startTime = System.currentTimeMillis();
//        System.out.println(System.currentTimeMillis() - startTime);
        System.out.println(prefixes.size());
        outputResult(prefixes);
    }
}
