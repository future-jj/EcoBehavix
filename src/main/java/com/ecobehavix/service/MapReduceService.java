package com.ecobehavix.service;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.annotation.Autowired;
// import org.apache.hadoop.mapred.JobConf;
import org.springframework.stereotype.Service;


@Service
public class MapReduceService {

    // @Autowired
    // private JobConf jobConf;

    // // 提交单词计数任务
    // public void runWordCountJob(String inputPath, String outputPath) 
    //         throws IOException, ClassNotFoundException, InterruptedException {
    //     // 使用新的API Job类时，需要从Configuration创建
    //     Job job = Job.getInstance(new Configuration(jobConf), "WordCount");
    //     job.setJarByClass(MapReduceService.class);

    //     job.setMapperClass(TokenizerMapper.class);
    //     job.setReducerClass(IntSumReducer.class);

    //     job.setOutputKeyClass(Text.class);
    //     job.setOutputValueClass(IntWritable.class);

    //     FileInputFormat.addInputPath(job, new Path(inputPath));
    //     FileOutputFormat.setOutputPath(job, new Path(outputPath));

    //     job.waitForCompletion(true);
    // }

    // // 自定义Mapper（使用新版API）
    // public static class TokenizerMapper 
    //         extends Mapper<Object, Text, Text, IntWritable> {
        
    //     private final static IntWritable one = new IntWritable(1);
    //     private final Text word = new Text();

    //     @Override
    //     public void map(Object key, Text value, Context context) 
    //             throws IOException, InterruptedException {
    //         StringTokenizer itr = new StringTokenizer(value.toString());
    //         while (itr.hasMoreTokens()) {
    //             word.set(itr.nextToken());
    //             context.write(word, one);
    //         }
    //     }
    // }

    // // 自定义Reducer（使用新版API）
    // public static class IntSumReducer 
    //         extends Reducer<Text, IntWritable, Text, IntWritable> {
        
    //     private IntWritable result = new IntWritable();

    //     @Override
    //     public void reduce(Text key, Iterable<IntWritable> values, Context context)
    //             throws IOException, InterruptedException {
    //         int sum = 0;
    //         for (IntWritable val : values) {
    //             sum += val.get();
    //         }
    //         result.set(sum);
    //         context.write(key, result);
    //     }
    // }
}
