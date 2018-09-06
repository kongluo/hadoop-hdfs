package com.richard.HdfsOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @program: hadoop-hdfs
 * @description:
 * @author: Mr.Wang
 * @create: 2018-09-06 16:29
 **/
public class Operation 
{
    FileSystem fileSystem = null;
    
    /**
    * @description: 获取文件系统  
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 16:36
    * @version: v1.0
    */
    @Before
    public void init() throws Exception{
        //1 获取文件系统
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://master:8020"),configuration,"root");
    }
    
    /**
    * @description: 上传文件 
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 16:32
    * @version: v1.0
    */
    @Test
    public void Upload() throws IOException {
        //2 把本地文件上传到文件系统中
        fileSystem.copyFromLocalFile(new Path("D:\\Study-Git\\hadoop-hdfs\\src\\main\\resources\\hello.txt"),new Path("/richard/hello1.txt"));

        //3 关闭资源
        fileSystem.close();
        System.out.println("over");
    }
    
    /**
    * @description: 下载文件  
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 16:34
    * @version: v1.0
    */
    @Test
    public void download() throws IOException {
        //2 下载文件到本地
        fileSystem.copyToLocalFile(new Path("hdfs://master:8020/richard/hello.txt"),new Path("D:\\Study-Git\\hadoop-hdfs\\src\\main\\resources\\download\\hello.txt"));
        //3 关闭
        fileSystem.close();
        System.out.println("over");
    }
    
    /**
    * @description: 创建目录  
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 16:35
    * @version: v1.0
    */
    @Test
    public void Mkdir() throws IOException {
        //2 创建目录
        fileSystem.mkdirs(new Path("hdfs://master:8020/richard/mkdir"));
        //3 关闭
        fileSystem.close();
        System.out.println("over");
    }
    
    /**
    * @description: 删除文件或文件夹  
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 16:35
    * @version: v1.0
    */
    @Test
    public void Delete() throws IOException {
        //2 删除文件或文件夹
        fileSystem.delete(new Path("hdfs://master:8020/richard/mkdir"),true);
        //3 关闭
        fileSystem.close();
        System.out.println("over");
    }

    /**
    * @description: 更改文件或文件夹名
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 16:45
    * @version: v1.0
    */
    @Test
    public void rename() throws IOException {
        //2 更名
        fileSystem.rename(new Path("hdfs://master:8020/richard/hello1.txt"),new Path("hdfs://master:8020/richard/hello2.txt"));
        //3 关闭
        fileSystem.close();
        System.out.println("over");
    }

    /**
    * @description: 查看文件详情
    * @param:
    * @author: richard.wang
    * @date: 2018/9/6 17:10
    * @version: v1.0
    */
    @Test
    public void readList() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/richard"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            // 获取文件名
            System.out.println(fileStatus.getPath().getName());
            // 获取块大小
            System.out.println(fileStatus.getBlockSize());
            // 获取文件权限
            System.out.println(fileStatus.getPermission());
            //字节长
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl:blockLocations){
                System.out.println("block-offset:"+bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host:hosts){
                    System.out.println(host);
                }
            }
            System.out.println("---------杨幂的发际线------------");
        }
    }

    /**
    * @description: 文件夹查看  
    * @param: 
    * @author: richard.wang
    * @date: 2018/9/6 17:12
    * @version: v1.0
    */
    @Test
    public void find() throws IOException {
        //2 查询路径下的文件状态信息
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/richard"));
        //3 遍历所有文件状态
        for (FileStatus status:fileStatuses){
            if (status.isFile()){
                System.out.println("f--"+status.getPath().getName());
            }else {
                System.out.println("d--"+status.getPath().getName());
            }
        }
    }
}
