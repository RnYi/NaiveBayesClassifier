import org.apache.commons.lang3.tuple.ImmutablePair;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.Math;

public class NaiveBayesPrediction {
    static Path localResultDir = Paths.get("/home/rany/Result");
    static Path localPredictionDir = Paths.get("/home/rany/NBCorpus/Country/Prediction");
    static Path classCountResult = localResultDir.resolve("ClassCount.txt");
    static Path wordCountResult = localResultDir.resolve("WordCount.txt");
    static Path docPredictionResult = localResultDir.resolve("DocPrediction.txt");

    //存储类别的先验概率
    private static HashMap<String, Double> priorProb = new HashMap<>();
    //存储单词在某个类别出现的次数
    private static HashMap<ImmutablePair<String,String>, Integer> wordCountInClass = new HashMap<>();
    //记录每种类别的单词总数（left）和单词集合大小（right）
    private static HashMap<String, ImmutablePair<Integer,Integer>> conditionProbInfo = new HashMap<>();

    static void PredictionPreHandle() throws IOException {
        CalculatePriorProb();
        GetConditionInfo();
    }

    static void GetConditionInfo() throws IOException {
        FileReader fr = new FileReader(wordCountResult.toFile());
        BufferedReader reader = new BufferedReader(fr);
        String line;
        String[] lineArg;
        //获取每个类别的单词总数和单词集合大小
        while((line=reader.readLine())!=null){
            lineArg = line.split("\t");
            wordCountInClass.put(new ImmutablePair(lineArg[0], lineArg[1]), new Integer(lineArg[2]));
            Integer countOfWords = new Integer(lineArg[2]);
            if(!conditionProbInfo.containsKey(lineArg[0])){
                conditionProbInfo.put(lineArg[0],
                        new ImmutablePair(countOfWords,1));
            }else{
                ImmutablePair<Integer, Integer> old = conditionProbInfo.get(lineArg[0]);
                countOfWords += old.left;
                Integer countOfWordSet = old.right;
                countOfWordSet++;
                conditionProbInfo.put(lineArg[0],
                        new ImmutablePair(countOfWords, countOfWordSet));
            }

        }
    }
    static void CalculatePriorProb() throws IOException {
        HashMap<String, Integer> map = new HashMap<>();
        FileReader fr = new FileReader(classCountResult.toFile());
        BufferedReader reader = new BufferedReader(fr);
        String line;
        String[] lineArg;
        int countOfDocs = 0;
        while((line = reader.readLine())!=null){
            lineArg = line.split("\t");
            Integer count = Integer.decode(lineArg[1]);
            map.put(lineArg[0],count);
            countOfDocs += count;
        }
        for(String className: map.keySet()){
            priorProb.put(className, (double)map.get(className)/countOfDocs);
        }
        reader.close();
        fr.close();

    }

    static double ProbForClassOfTheDoc(String className, ArrayList<String> content){
        double cmap = Math.log(priorProb.get(className).doubleValue());
        for(String word: content){
            ImmutablePair tc = new ImmutablePair(className, word);
            ImmutablePair<Integer, Integer> cpinfo = conditionProbInfo.get(className);
            double pTc = (double)(wordCountInClass.getOrDefault(tc,0)+1)/(cpinfo.left+cpinfo.right);
            cmap += Math.log(pTc);
        }
        return cmap;
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        PredictionPreHandle();
        int success = 0;
        int countOfPredictionSet = 0;
        //对每个类别的预测集进行预测
        ArrayList<String> classNames = new ArrayList<>();
        for(File dir: localPredictionDir.toFile().listFiles()){
             classNames.add(dir.getName());
        }
        for(File dir: localPredictionDir.toFile().listFiles()){
            String realClass = dir.getName();
            for(File sample: dir.listFiles()){
                FileReader fr = new FileReader(sample);
                BufferedReader reader = new BufferedReader(fr);
                String line;
                ArrayList<String> wordList = new ArrayList<>();
                while((line=reader.readLine())!=null){
                    wordList.add(line);
                }
                String predictionClass ="";
                double maxProb = Double.NEGATIVE_INFINITY;
                for(String className: classNames){
                    double prob = ProbForClassOfTheDoc(className, wordList);
                    if(maxProb<prob){
                        predictionClass = className;
                        maxProb = prob;
                    }
                }
                if(predictionClass.equals(realClass)){
                    success++;
                }
                countOfPredictionSet++;
                System.out.println(sample.getPath() + "\t" + predictionClass + "\t" + realClass);

            }
        }
        System.out.println("总数：" + countOfPredictionSet + "\t" +
                "预测正确：" + success + "\t" +
                "正确率：" + (double)success/countOfPredictionSet);
    }
}
