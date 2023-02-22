// NAME: TAM ZHER MIN
// MATRICULATION NUMBER: A0206262N
// TopkCommonWords.java

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopkCommonWords {

  public static class File1Mapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private Set<String> stopwords;
    private Text word = new Text();
    private IntWritable count = new IntWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
      stopwords = getStopwords(context.getConfiguration());
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      HashMap<String, Integer> wordcount = getWordcount(value, stopwords);

      for (String mapKey : wordcount.keySet()) {
        word.set(mapKey);
        count.set(wordcount.get(mapKey));
        context.write(word, count);
      }
    }
  }

  public static class File2Mapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private Set<String> stopwords;
    private Text word = new Text();
    private IntWritable count = new IntWritable();

    protected void setup(Context context) throws IOException, InterruptedException {
      stopwords = getStopwords(context.getConfiguration());
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      HashMap<String, Integer> wordcount = getWordcount(value, stopwords);

      for (String mapKey : wordcount.keySet()) {
        word.set(mapKey);
        count.set(-1 * wordcount.get(mapKey));
        context.write(word, count);
      }
    }
  }

  public static class InnerJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum1 = 0, sum2 = 0;
      for (IntWritable val : values) {
        if (val.get() < 0) {
          sum1 += (-1 * val.get());
        } else {
          sum2 += val.get();
        }
      }

      if (sum1 < sum2) {
        map.put(key.toString(), sum1);
      } else {
        map.put(key.toString(), sum2);
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Text result = new Text();

      List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
      Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
          int compare = o2.getValue().compareTo(o1.getValue());
          if (compare != 0) {
            return compare;
          }
          return o1.getKey().compareTo(o2.getKey());
        }
      });

      for (int i = 0; i < Integer.parseInt(conf.get("k")); i++) {
        result.set(list.get(i).getValue() + "\t" + list.get(i).getKey());
        context.write(result, null);
      }
    }
  }

  private static HashSet<String> getStopwords(Configuration conf) throws IOException {
    HashSet<String> stopwords = new HashSet<String>();

    FileSystem fs = FileSystem.get(conf);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(conf.get("stopwords")))));
    String line;
    while ((line = reader.readLine()) != null) {
      stopwords.add(line);
    }
    reader.close();

    return stopwords;
  }

  private static HashMap<String, Integer> getWordcount(Text value, Set<String> stopwords) throws IOException {
    HashMap<String, Integer> wordcount = new HashMap<String, Integer>();

    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String token = itr.nextToken();
      if (token.length() < 5 || stopwords.contains(token))
        continue;
      if (!wordcount.containsKey(token)) {
        wordcount.put(token, 1);
      } else {
        wordcount.put(token, wordcount.get(token) + 1);
      }
    }

    return wordcount;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("stopwords", args[2]);
    conf.set("k", args[4]);

    Job job = Job.getInstance(conf, "topkcommonwords");
    job.setJarByClass(TopkCommonWords.class);

    job.setReducerClass(InnerJoinReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, File1Mapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, File2Mapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
