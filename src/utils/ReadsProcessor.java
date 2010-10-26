package utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import common.SeedMaker;
import common.Utils;

public class ReadsProcessor {

	static byte[] compressReads(char[] read, int max_diffs){		
		int byte_num = 1;
		if (read.length>SeedMaker.max_seed_part)
			byte_num = (int) Math.ceil(read.length/4.0);
		//System.out.println("byte num"+byte_num);
		byte[] result = new byte[byte_num];		
//		we don't care read length here since it is passed in the conf of the job
		// clean the read
		char[] cleanedread = new char[read.length];
		int counter = 0;
		for (int i=0;i<read.length;i++){
			if (!Utils.isvalid(read[i])){			
				counter++;
				cleanedread[i] = 'A';
			} else cleanedread[i] = read[i];
		}
		if (counter > max_diffs) return null;
		int i=0;
		for (int j=0;j<byte_num;j++){	
			i = j*4;
			result[j]=0;
			while (i<read.length && i<(j+1)*4){
				//Byte.SIZE/2==4	
				//System.out.println(cleanedread[i]);
				result[j] = (byte)((result[j] << 2) + Utils.base2int(cleanedread[i]));
				i++;
			}
			//System.out.println(result[j]);
		}

		return result;
	}
	static byte[] decompress(byte[] arr, int read_length){
		//after compressing, all reads can be considered as clean
		//unidentify char Ns were replaced by As
		byte[] result = new byte[read_length];
		//System.out.println("arr length="+arr.length);
		int i;
		for (int j=0;j<arr.length;j++){	
			i = j*4;
			while (i<read_length && i<(j+1)*4){
				result[i] = (byte)((arr[j] & 0xC0)>>>6);
				//System.out.println(result[i]);
				arr[j] = (byte)(arr[j]<<2);
				i++;
			}
		}
		
		return result;
	}
	private static Text iw = new Text();
	
	public static void convertFile(String infile, String outfile,int max_diff) throws IOException
	{
		String header = "";
		Configuration config = new Configuration();
		//config.set("mapred.job.tracker", "local");
		//config.set("fs.default.name", "file:///");

		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(outfile), Text.class, BytesWritable.class);		
		int count = 0;		
		try{
			BufferedReader data = 
				new BufferedReader(new InputStreamReader(new FileInputStream(infile)));			    
			String line;
			char[] chararr = null;
			byte[] compressedarr = null;
			while ((line = data.readLine()) != null){
				line.trim();				
				if (line.isEmpty())	{
					// Guard against empty lines
					continue;
				}				
				if (line.charAt(0) == '>')
				{					
					if (count > 0)
					{
						iw.set(header);
						if ((compressedarr=compressReads(chararr,max_diff))!=null)
						writer.append(iw, 
								new BytesWritable(compressedarr));
					}			
					header = line.substring(1); // skip the >
					count++;											
				}
				else
				{
					chararr = line.toUpperCase().toCharArray();
					//System.out.println(line);
					//sequence.append(line.toUpperCase());				 
				}
			}			
			iw.set(header);
			if ((compressedarr=compressReads(chararr,max_diff))!=null)
				writer.append(iw, 
						new BytesWritable(compressedarr));
			writer.close();
		} 
		catch (FileNotFoundException e) 
		{
			System.err.println("Can't open " + infile);
			e.printStackTrace();
			System.exit(1);
		}
		
		System.err.println("Processed " + count + " sequences");
	}
	public static void printFile(String infile, String out,int read_length) throws IOException
	{
		Path thePath = new Path(infile);
		Configuration conf = new Configuration(); 
		SequenceFile.Reader theReader = 
			new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
	       
	    Text key = new Text();
	    BytesWritable value = new BytesWritable();
	    FileWriter fw = new FileWriter(out);
	    byte[] readline;
	    while(theReader.next(key,value))
	    {
	    	fw.write(key.toString());
	    	fw.write("\n");
	    	readline = decompress(value.getBytes(),read_length);
	    	for (int i=0;i<readline.length;i++)
	    		fw.write(readline[i]);
	    	fw.write("\n");
	    }
	    fw.close();
	}
	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.err.println("Usage: ReadProcessor infile outfile checkfile");
			System.exit(-1);
		}		
		String infile = args[0];
		String outfile = args[1];
		String checkfile = args[2];
					
		System.err.println("Converting " + infile + " into " + outfile);
		
		convertFile(infile, outfile, 3);
		
		printFile(outfile, checkfile, 36);//FIXME: change this accordingly
	}

}
