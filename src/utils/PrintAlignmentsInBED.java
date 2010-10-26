package utils;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import common.GenomicRegionWritable;

public class PrintAlignmentsInBED {
	
	private static Configuration conf = null;
	
	public static void printFile(Path thePath, String out) throws IOException
	{
		SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
	       
	    Text key = new Text();
	    GenomicRegionWritable value = new GenomicRegionWritable();
	    FileWriter fw = new FileWriter(out);
	    
	    while(theReader.next(key,value))
	    {
	    	System.out.println(value.toString());
	    	 fw.write(value.toString());
	    	 fw.write("\n");
	    }
	    fw.close();
	}

	public static void main(String[] args) throws IOException 
	{
		String filename = null;
		String outfile = "results_cloud.bed";
		if (filename == null)
		{
			if (args.length != 2) 
			{
				System.err.println("Usage: PrintAlignments seqfile outfile");
				System.exit(-1);
			}
			filename = args[0];
			outfile = args[1];//this is supposed to be on local file system
		}
			
		System.err.println("Printing " + filename);
		
		Path thePath = new Path(filename);
	    //conf = new JobConf(AlignmentStats.class);
	    conf = new Configuration(); 
	    FileSystem fs = FileSystem.get(conf);
	       
	    if (!fs.exists(thePath))
	    {
	    	throw new IOException(thePath + " not found");   
	    }

	    FileStatus status = fs.getFileStatus(thePath);

	    if (status.isDir())
	    {    	   
	    	FileStatus [] files = fs.listStatus(thePath);
	    	for(FileStatus file : files)
	    	{
	    		String str = file.getPath().getName();

	    		if (str.startsWith("."))
	    		{
	    			// skip
	    		}			   
	    		else if (!file.isDir())
	    		{
	    			printFile(file.getPath(),outfile);
	    		}
	    	}
	    }
	    else
	    {
	    	printFile(thePath,outfile);	   
	    }
	}
}

