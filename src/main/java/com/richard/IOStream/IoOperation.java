package com.richard.IOStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @program: hadoop-hdfs
 * @description:
 * @author: Mr.Wang
 * @create: 2018-09-06 17:19
 **/
public class IoOperation
{
    /**
     * @description: 获取文件系统
     * @param:
     * @author: richard.wang
     * @date: 2018/9/6 16:36
     * @version: v1.0
     */
    FileSystem fileSystem = null;

    @Before
    public void init() throws Exception{
        //1 获取文件系统
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://master:8020"),configuration,"root");
    }

    /**
    * @description: 通过IO流的方式上传文件
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 17:25
    * @version: v1.0
    */
    @Test
    public void putFileToHDFS() throws IOException {
        //2 创建输入流
        FileInputStream inStream = new FileInputStream(new File("D:\\Study-Git\\hadoop-hdfs\\src\\main\\resources\\upload\\hadoop-2.6.0-cdh5.11.0.tar.gz"));

        //3 获取输出路径
        String putFileName = "hdfs://master:8020/richard/hadoop-2.6.0-cdh5.11.0.tar.gz";
        Path writepath = new Path(putFileName);

        //4 创建输出流
        FSDataOutputStream outStream = fileSystem.create(writepath);

        //5 流对接
        try {
            IOUtils.copyBytes(inStream,outStream,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }

    /**
    * @description: 通过IO流的方式下载文件
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 17:32
    * @version: v1.0
    */
    @Test
    public void getFileToHDFS() throws IOException {
        //2 获取读取文件路径
        String fileName = "hdfs://master:8020/richard/hello.txt";

        //3 创建读取path
        Path readpath = new Path(fileName);

        //4 创建输入流
        FSDataInputStream inStream = fileSystem.open(readpath);

        //5 流对接输出到控制台
        try {
            IOUtils.copyBytes(inStream,System.out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(inStream);
        }

    }

    /**
    * @description: 下载第一块
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 17:41
    * @version: v1.0
    */
    @Test
    public void readFileSeek1() throws IOException {
        //2 获取输入流路径
        Path path = new Path("hdfs://master:8020/richard/hadoop-2.6.0-cdh5.11.0.tar.gz");
        //3 打开输入流
        FSDataInputStream fis = fileSystem.open(path);
        //4 创建输出流
        FileOutputStream fos = new FileOutputStream("D:\\Study-Git\\hadoop-hdfs\\src\\main\\resources\\download\\hadoop-2.6.0-cdh5.11.0.tar.gz.part1");
        //5 流对接
        byte[] buf = new byte[1024];
        for (int i=0;i<128*1024;i++){
            fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
    * @description: 下载第二块
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 17:46
    * @version: v1.0
    */
    @Test
    public void readFileSeek2() throws IOException {
        //2 获取输入流路径
        Path path = new Path("hdfs://master:8020/richard/hadoop-2.6.0-cdh5.11.0.tar.gz");
        //3 打开输入流
        FSDataInputStream fis = fileSystem.open(path);
        //4 打开输出流
        FileOutputStream fos = new FileOutputStream("D:\\Study-Git\\hadoop-hdfs\\src\\main\\resources\\download\\hadoop-2.6.0-cdh5.11.0.tar.gz.part2");
        //5 定位偏移量(第二块的首位)
        fis.seek(1024*1024*128);

        //6 流对接
        IOUtils.copyBytes(fis,fos,1024);

        //7 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    //合并文件
    /*
    *  type hadoop-2.6.0-cdh5.11.0.tar.gz.part3 >> hadoop-2.6.0-cdh5.11.0.tar.gz.part2 >> hadoop-2.6.0-cdh5.11.0.tar.gz.part1
    * */
}
