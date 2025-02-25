package com.ecobehavix.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import com.ecobehavix.EcobehavixApplication;

import java.io.File;
import java.io.IOException;
import java.net.URI;


@org.springframework.context.annotation.Configuration
public class HadoopConfig {

    @Value("${hadoop.fs.defaultFS}")
    private String hdfsUri;

    @Value("${hadoop.yarn.resourcemanager.address}")
    private String resourceManagerAddress;

    @Bean
    public Configuration hadoopConfiguration() {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsUri);
        config.set("mapreduce.framework.name", "yarn");
        config.set("yarn.resourcemanager.address", resourceManagerAddress);
        config.set("mapreduce.app-submission.cross-platform", "true");
        config.set("mapreduce.jobhistory.address", "master:10020");
        return config;
    }

    @Bean
    public FileSystem fileSystem() throws IOException, InterruptedException {
        return FileSystem.get(URI.create(hdfsUri), hadoopConfiguration(), "hadoop");
    }

    @Bean
    public JobConf jobConf() {
        JobConf conf = new JobConf(hadoopConfiguration());
        conf.setJarByClass(EcobehavixApplication.class);
        return conf;
    }

}
