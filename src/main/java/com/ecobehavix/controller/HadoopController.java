package com.ecobehavix.controller;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ecobehavix.mapreduce.WordCountMapper;
import com.ecobehavix.mapreduce.WordCountReducer;

@RestController
@RequestMapping("/hadoop")
public class HadoopController {

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private JobConf jobConf;

    @PostMapping("/upload")
    public String uploadFile(@RequestParam String localPath,
                            @RequestParam String hdfsPath) throws IOException {
        Path local = new Path(localPath);
        Path hdfs = new Path(hdfsPath);
        fileSystem.copyFromLocalFile(local, hdfs);
        return "File uploaded to HDFS: " + hdfsPath;
    }

    @PostMapping("/run-wordcount")
    public String runWordCount(@RequestParam String inputPath,
                              @RequestParam String outputPath) throws IOException {
        JobConf conf = new JobConf(jobConf);
        conf.setJobName("WordCount");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
        return "MapReduce job submitted. Output at: " + outputPath;
    }
}