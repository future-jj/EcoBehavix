package com.ecobehavix.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HdfsService {
    
    @Autowired
    private FileSystem fs;

    // 上传本地文件到HDFS
    public void uploadFile(String localPath, String hdfsPath) throws IOException {
        fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
    }

    // 从HDFS下载文件到本地
    public void downloadFile(String hdfsPath, String localPath) throws IOException {
        fs.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
    }

    // 查看HDFS目录内容
    public List<String> listFiles(String hdfsDir) throws IOException {
        return Arrays.stream(fs.listStatus(new Path(hdfsDir)))
                   .map(FileStatus::getPath)
                   .map(Path::toString)
                   .collect(Collectors.toList());
    }
}
