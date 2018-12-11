package io.arabesque.search.steps;

import io.arabesque.*;
import io.arabesque.conf.Configuration;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.BasicMainGraphQuery;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class GenericJoin
        implements Function2<Integer, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>>, Iterator<Integer>> {
    private static final Logger LOG = Logger.getLogger(GenericJoin.class);

    private Broadcast<SparkConfiguration> configBC;
    private Broadcast<QueryGraph> queryGraphBC;

    // accumulators
    private Map<String, CollectionAccumulator<Long>> accums;
    private long init_Start_Time = 0;
    private long init_Finish_Time = 0;
    private long computation_Start_Time = 0;
    private long computation_Finish_Time = 0;

    public GenericJoin(Broadcast<SparkConfiguration> configBC, Broadcast<QueryGraph> queryGraphBC,
                       Map<String, CollectionAccumulator<Long>> _accums){
        super();

        init_Start_Time = System.currentTimeMillis();

        this.configBC = configBC;
        this.queryGraphBC = queryGraphBC;

        this.accums = _accums;

        String log_level = this.configBC.value().getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
    }

    @Override
    public Iterator<Integer> call(Integer partitionId, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>> v2) throws Exception {
//        if (v2 != null && v2.hasNext()){
//
//            configBC.value().initialize();
//
//            Tuple2<Integer, Iterable<SearchDataTree>> iter = v2.next();
//            Iterator<SearchDataTree> msgs = iter._2().iterator();
//
//            UnsafeCSRGraphSearch dataGraph = io.arabesque.conf.Configuration.get().getSearchMainGraph();
//            QueryGraph queryGraph = queryGraphBC.getValue();
//
//            init_Finish_Time = System.currentTimeMillis();
//            computation_Start_Time = System.currentTimeMillis();
//
//            ThreadOutput out = ThreadOutputHandler.createThreadOutput(partitionId, true);
//            CrossEdgeMatchingLogic crossEdgeMatchingLogic = new CrossEdgeMatchingLogic(dataGraph, queryGraph, out);
//
//            while(msgs.hasNext()) {
//                SearchDataTree split = msgs.next();
//                split.setSearchQueryTree(queryGraph);
//                crossEdgeMatchingLogic.matchCrossEdges(split);
//            }
//
//            ThreadOutputHandler.closeThreadOutput(out);
//        }

        configBC.value().initialize();

        QueryGraph queryGraph = queryGraphBC.getValue();

        init_Finish_Time = System.currentTimeMillis();
        computation_Start_Time = System.currentTimeMillis();

        Configuration arabesque_conf = Configuration.get();
        String OUTPUT_PATH  = arabesque_conf .getString(arabesque_conf .SEARCH_OUTPUT_PATH, arabesque_conf .SEARCH_OUTPUT_PATH_DEFAULT);
        //org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
       // conf.set("fs.defaultFS","hdfs://localhost:8020");
//        FileSystem fs = null;
//        try {
//            fs = FileSystem.get(conf);
//        } catch (IOException e) {
//            LOG.error("Could not get the file system");
//            e.printStackTrace();
//            System.exit(1);
//        }
//        fs.create(new Path(OUTPUT_PATH, Integer.toString(partitionId)));
//        fs.close();

        ThreadOutput out = ThreadOutputHandler.createThreadOutput(partitionId, false);

        UnsafeCSRGraphSearch dataGraph = io.arabesque.conf.Configuration.get().getSearchMainGraph();
        BasicMainGraph dataGraphU = io.arabesque.conf.Configuration.get().getSearchMainGraphU();

        int numLabels = arabesque_conf.getInteger(arabesque_conf.SEARCH_NUM_LABELS, arabesque_conf.SEARCH_NUM_LABELS_DEFAULT);

        AbstractGenericJoin gj = null;
        if(numLabels <= 1) {
            int numVertices = arabesque_conf.getInteger(arabesque_conf.SEARCH_NUM_VERTICES, arabesque_conf.SEARCH_NUM_VERTICES_DEFAULT);
            gj = new GJUnlabelled(queryGraph, dataGraph, numVertices, out);
        }
        else
            gj = new GJLabelled(queryGraph, dataGraph, out);
        init_Finish_Time = System.currentTimeMillis();
        computation_Start_Time = System.currentTimeMillis();

        //GJLabelled gjLabelled = new GJLabelled(queryGraph, dataGraph, out);
        gj.search();

        ThreadOutputHandler.closeThreadOutput(out);

        // TODO we actually don't need to output an RDD. All matches are stored to HDFS.
        ArrayList<Integer> list = new ArrayList();
        list.add(0);

        computation_Finish_Time = System.currentTimeMillis();
        flushAccumulators();

        return list.iterator();
    }

    private void accumulate(Long value, CollectionAccumulator<Long> _accum) {
        _accum.add( value );
    }

    private void flushAccumulators() {
        // Now calculate the computation runtime
        accumulate(init_Start_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_INIT_START_TIME));
        accumulate(init_Finish_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_INIT_FINISH_TIME));
        accumulate(computation_Start_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_COMPUTATION_START_TIME));
        accumulate(computation_Finish_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME));
    }
}
