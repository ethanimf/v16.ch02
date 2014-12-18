package ethan.v16.ch02.mr.taobao.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class HDFSTest01 {  
      
    /** 
     * 文件上传 
     * @param src  
     * @param dst 
     * @param conf 
     * @return 
     */  
    public static boolean put2HSFS(String src , String dst , Configuration conf){  
        Path dstPath = new Path(dst) ;  
        try{  
            FileSystem hdfs = dstPath.getFileSystem(conf) ;  
//          FileSystem hdfs = FileSystem.get( URI.create(dst), conf) ;   
            hdfs.copyFromLocalFile(false, new Path(src), dstPath) ;  
        }catch(IOException ie){  
            ie.printStackTrace() ;  
            return false ;  
        }  
        return true ;  
    }  
      
    /** 
     * 文件下载 
     * @param src 
     * @param dst 
     * @param conf 
     * @return 
     */  
    public static boolean getFromHDFS(String src , String dst , Configuration conf){  
        Path dstPath = new Path(dst) ;  
        try{  
            FileSystem dhfs = dstPath.getFileSystem(conf) ;  
            dhfs.copyToLocalFile(false, new Path(src), dstPath) ;  
        }catch(IOException ie){  
            ie.printStackTrace() ;  
            return false ;  
        }  
        return true ;  
    }  
      
    /** 
     * 文件检测并删除 
     * @param path 
     * @param conf 
     * @return 
     */  
    public static boolean checkAndDel(final String path , Configuration conf){  
        Path dstPath = new Path(path) ;  
        try{  
            FileSystem dhfs = dstPath.getFileSystem(conf) ;  
            if(dhfs.exists(dstPath)){  
                dhfs.delete(dstPath, true) ;  
            }else{  
                return false ;  
            }  
        }catch(IOException ie ){  
            ie.printStackTrace() ;  
            return false ;  
        }  
        return true ;  
    }  
  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
        String dst = "/vms/v16/aa" ;  
    	String output = "hdfs://u1:9000/home/hadoop/out.v16/*";
        boolean status = false ;  
          
          
//        Configuration conf = new Configuration() ;  
        JobConf conf = new JobConf();
//        status = put2HSFS( src ,  dst ,  conf) ;  
//        System.out.println("status="+status) ;  
          
        status = getFromHDFS( output ,  dst ,  conf) ;  
        System.out.println("status="+status) ;  
          
//        status = checkAndDel( dst ,  conf) ;  
//        System.out.println("status="+status) ;  
    }  
  
  
}  