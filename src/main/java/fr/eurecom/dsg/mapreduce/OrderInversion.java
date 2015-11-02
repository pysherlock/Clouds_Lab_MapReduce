package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


public class OrderInversion extends Configured implements Tool {

  private final static String ASTERISK = "\0";

  public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {
    @Override
    public int getPartition(TextPair key, IntWritable value, int numPartitions) {
      // TODO: implement getPartition such that pairs with the same first element
      //       will go to the same reducer. You can use toUnsighed as utility.
      return key.getFirst().hashCode()%numPartitions; // send the TextPair with the same left word to the same reducer
    }
    
    /**
     * toUnsigned(10) = 10
     * toUnsigned(-1) = 2147483647
     * 
     * @param val Value to convert
     * @return the unsigned number with the same bits of val 
     * */
    public static int toUnsigned(int val) {
      return val & Integer.MAX_VALUE;
    }
  }

  public static class PairMapper extends
  Mapper<LongWritable, Text, TextPair, IntWritable> {

    private int window = 2; //It should be modified!!!!!
    private int sum = 0;
    private static IntWritable ONE = new IntWritable(1);
    private static TextPair textPair = new TextPair();
    private static TextPair SpecialTP = new TextPair();
    private String pattern = "[^a-zA-Z0-9]";

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws java.io.IOException, InterruptedException {
      window = context.getConfiguration().getInt("window", 2);
      String line = value.toString();
      line = line.replaceAll(pattern, " ");
      String[] words = line.split("\\s+"); //split string to tokens
      for(int i = 0; i < words.length; i++) {
        /*
        for(int j = 0; j < words.length; j++) {
          if(i == j)
            continue;
          else if (words[j].length() == 0)
            continue;
          else{
            textPair.set(new Text(words[i]), new Text(words[j]));
            context.write(textPair, ONE);
            sum += 1;
          }
        }*/

        for(int j = i - window; j < i + window + 1; j++) {
          if(i == j || j < 0)
            continue;
          else if(j >= words.length)
            break;
          else if (words[j].length() == 0) //skip empty tokens
            break;
          else{
            textPair.set(new Text(words[i]), new Text(words[j]));
            context.write(textPair, ONE);
            sum++;
          }
        }
        SpecialTP.set(new Text(words[i]), new Text("*"));
        context.write(SpecialTP, new IntWritable(sum));
      }
    }
  }

  public static class PairReducer extends
  Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

    private static DoubleWritable SumValue = new DoubleWritable(0);
    private static Text Current = new Text("NOT_SET");
    private int total = 0;

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

      Iterator<IntWritable> iter = values.iterator();

      if (!key.getFirst().equals(Current)) {
        Current = new Text(key.getFirst());
        total = 0;
      } // When TextPair's left word changes
      int count = 0;
      while (iter.hasNext()) {
        if(key.getSecond().toString().equals("*"))
          total += iter.next().get();
        else
          count += iter.next().get();
      }
      if (count != 0) {
        double divide = (double)count / total;
        SumValue.set(divide);
      }
      else if(key.getSecond().toString().equals("*"))
        SumValue.set(total);

      context.write(key, SumValue);
    }
    // TODO: implement the reduce method
  }

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    Job job = new Job(conf, "OrderInversion");


    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(PairMapper.class);
    job.setMapOutputKeyClass(TextPair.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(PairReducer.class);
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(IntWritable.class); //how to build the matrix ???

    job.setOutputFormatClass(TextOutputFormat.class);

    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[1])); //Why?? from different lib
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setNumReduceTasks(Integer.parseInt(args[0]));

    job.setJarByClass(OrderInversion.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  OrderInversion(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
    System.exit(res);
  }
}
