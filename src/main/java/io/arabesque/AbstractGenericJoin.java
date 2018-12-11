package io.arabesque;

import io.arabesque.search.trees.SearchEmbedding;
import io.arabesque.utils.ThreadOutput;

import java.io.IOException;
import java.util.*;

public abstract class AbstractGenericJoin {
    private ThreadOutput outputStream;

    public AbstractGenericJoin(ThreadOutput outputStream){
        this.outputStream = outputStream;
    }

    public abstract void search();

    public void outputResult(Deque<PrefixTuple> q){
        int count = 0;
        Set<String> set = new HashSet<>();
        while (!q.isEmpty()){

            PrefixTuple p = q.poll();
            List<Integer> res = new ArrayList<>(p.map.values());
            Collections.sort(res);
            if(set.add(res.toString())){
                try {
                    SearchEmbedding searchEmbedding = new SearchEmbedding();
                    for(Integer i : res){
                        searchEmbedding.addWord(i);
                    }
                    if(searchEmbedding.getSize() > 0){
                        outputStream.write(searchEmbedding);
                    }

                } catch (IOException e) {
                    System.out.println("Could not write embeddings to Thread Output. Exiting.");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public Set<Integer> getIntersection(PriorityQueue<Set<Integer>> queue){
        Set<Integer> min = new HashSet<>(queue.poll());
        while (!queue.isEmpty()){
            min.retainAll(queue.poll());
            if(min.size() <= 0)
                break;
        }
        return min;
    }
}
