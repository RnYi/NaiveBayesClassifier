import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class NaiveBayesPrediction {
    //HDFS的URI
    static String hdfsURI = "hdfs://localhost:9000";
    //HDFS上结果路径前缀
    static Path resultPrefixOnHDFS = new Path("/Result");

    private Map<String, Double> priorProb;
    private HashMap<Pair<String,String>, Double> conditionProb;
    private HashMap<String, Pair<Integer,Integer>> conditionProbInfo;

    static void PredictionPreHandle() throws URISyntaxException, IOException {
        FileSystem hdfs = FileSystem.get(new URI(hdfsURI), new Configuration());
        Path classCountPath = new Path(resultPrefixOnHDFS, "ClassCount");
        Path wordCountPath = new Path(resultPrefixOnHDFS, "WordCount");
        CalculatePriorProb(hdfs, classCountPath);
        GetConditionProbInfo(hdfs, wordCountPath);
        CalculateConditionProb(hdfs, wordCountPath);
    }

    static void GetConditionProbInfo(FileSystem fileSystem, Path path){

    }

    static void CalculatePriorProb(FileSystem fileSystem, Path path){

    }
    static void CalculateConditionProb(FileSystem fileSystem, Path path){

    }

    static double ProbForClass(String className, String content){
        /* TODO */
        return 0;
    }

    public static void main(String[] args) {

    }
}
