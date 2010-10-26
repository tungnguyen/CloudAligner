package utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import common.DNAString;

public class ConvertFastaReadFile {
	public static int READS_PER_PARTITION = 50;//number of read*read_width should = 64MB (1 block)
/*****************************************
 * To improve the performance, we divide the read file into many partitions.
 * This is the number of reads per partition. This differs from the READS_PER_ROUND in CloudSMAP
 * For each partition we may process many rounds.
 * For example: If READS_PER_PARTITION = 10000 and READS_PER_ROUND=1000, we have 10 rounds
*********************************************/

	private static Text key = new Text();

	public static void convertFile(String infile,String outfile) throws IOException
	{
		String header = "";
		StringBuilder sequence = null;		
		int count = 0;		
		try{
			Configuration config = new Configuration();			
			SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
					new Path(outfile), Text.class, BytesWritable.class);			
			BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));			
			    
			String line;
			while ((line = data.readLine()) != null) 
			{
				line.trim();				
				if (line.isEmpty())
				{
					// Guard against empty lines
					continue;
				}				
				if (line.charAt(0) == '>')
				{					
					if (count > 0)
					{
						key.set(header);
						writer.append(key, new BytesWritable(DNAString.stringToBytes(sequence.toString())));
//						if (count % READS_PER_PARTITION == 0)
//							writer.sync();
					}			
					sequence = new StringBuilder();
					header = line.substring(1); // skip the >
					count++;											
				}
				else
				{
					sequence.append(line.toUpperCase());				 
				}
			}			
			key.set(header);
			writer.append(key,	new BytesWritable(DNAString.stringToBytes(sequence.toString())));
			writer.sync();
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
	
	public static void main(String[] args) throws IOException 
	{
		if (args.length < 2) {
			System.err.println("Usage: ConvertFastaReadFile infile outfile");
			System.exit(-1);
		}		
		String infile = args[0];
		String outfile = args[1];
		convertFile(infile,outfile);		
	}
}