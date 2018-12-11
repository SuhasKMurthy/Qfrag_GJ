package io.arabesque;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntIterator;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntConsumer;
import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.graph.Vertex;
import io.arabesque.search.steps.QueryGraph;
import io.arabesque.search.trees.SearchEmbedding;
import io.arabesque.utils.ThreadOutput;

import java.io.IOException;
import java.util.*;

public class GJUnlabelled extends AbstractGenericJoin {
    QueryGraph queryGraph = null;
    UnsafeCSRGraphSearch dataGraph = null;
    private int numVertices;
    private IntSet neigh;
    private IntConsumer addToNeigh;

    public GJUnlabelled(QueryGraph queryGraph, UnsafeCSRGraphSearch dataGraph,
                        int numVertices, ThreadOutput outputStream){
        super(outputStream);
        this.dataGraph = dataGraph;
        this.queryGraph = queryGraph;
        this.numVertices = numVertices;
        neigh = HashIntSets.newMutableSet(32);
        addToNeigh = new com.koloboke.function.IntConsumer() {
            @Override
            public void accept(int word) {
                neigh.add(word);
            }
        };
    }

    public void search(){
        Deque<PrefixTuple> prefixes = new ArrayDeque<>();

        Vertex[] queryVertices = queryGraph.queryGraph.getVertices();

        Vertex[] allVertices = dataGraph.getVertices();
        Set<Integer> allVerticesSet = new HashSet<>();

        for(int i = 0; i < numVertices; i++){
            int currVertex = i;
            PrefixTuple prefixTuple = new PrefixTuple();
            prefixTuple.addToMap(queryVertices[0].getVertexId(), currVertex);
            prefixes.addLast(prefixTuple);
            allVerticesSet.add(i);
        }

        for(int i = 1; i < queryGraph.getNumberVertices(); i++){
            int size = prefixes.size();

            if(queryGraph.queryGraph.getVertexNeighbours(i) == null)
                continue;

            int[] vertices = queryGraph.queryGraph.getVertexNeighbours(i).toIntArray();

            for(int j = 0; j < size; j++){
                PrefixTuple curr = prefixes.poll();


                Set<Integer> currSet = new HashSet<>(curr.map.values());


                PriorityQueue<Set<Integer>> extensions =
                        new PriorityQueue<>((a, b) -> a.size() - b.size());
                boolean isEmpty = false;
                for(Integer v : vertices){
                    Integer val = curr.map.get(v);
                    if(val == null){
                        extensions.add(allVerticesSet);
                    } else {
                        //IntCollection neigh = dataGraph.getVertexNeighbours(val);
                        neigh.clear();
                        dataGraph.processVertexNeighbors(val, addToNeigh);
                        neigh.forEach(addToNeigh);
                        if(neigh == null){
                            isEmpty = true;
                            break;
                        }
                        Set<Integer> newSet = new HashSet<>();
                        for(Integer pot : neigh){
                            newSet.add(pot);
                        }
                        extensions.add(newSet);
                    }
                }
                if(isEmpty)
                    continue;
                Set<Integer> next = getIntersection(extensions);
                for(Integer n : next){
                    if(currSet.contains(n))
                        continue;
                    PrefixTuple newTuple = new PrefixTuple();
                    newTuple.map = new HashMap<>(curr.map);
                    newTuple.addToMap(i, n);
                    prefixes.addLast(newTuple);
                }
            }
        }
        outputResult(prefixes);
    }
}
