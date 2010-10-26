package utils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import common.DNAString;
import common.FastaRecordWritable;

public class ConvertReferenceFile {	

	public static int CHUNK_OVERLAP = 1024;
	public static int CHUNK_SIZE=65535;		

	public static int min_seq_len = Integer.MAX_VALUE;
	public static int max_seq_len = 0;

	public static int min(int a, int b)
	{			
		if (a < b) return a;
		return b;
	}

	private static IntWritable iw = new IntWritable();

	public static void saveSequence(String name, StringBuilder sequence, Writer writer) throws IOException
	{	
		int fulllength = sequence.length();			
		if (fulllength < min_seq_len) { min_seq_len = fulllength; }
		if (fulllength > max_seq_len) { max_seq_len = fulllength; }
		int offset = 0;
		int numchunks = 0;
		while(offset < fulllength)
		{
			numchunks++;
			int end = min(offset + CHUNK_SIZE, fulllength);				
//			boolean lastChunk = (end == fulllength);
			/**********
			 * two mappers for each partition of the reference file
			 * one mapper processes even partitions
			 * another processes odd partitions
			 * */
			iw.set(offset);
			writer.append(iw, 
					new FastaRecordWritable(DNAString.stringToBytes(sequence.substring(offset, end)),offset));
			iw.set(offset+1);//TRICKY here!! if original key (offset) is odd then its replica here has key which is even and versi versa
			writer.append(iw, 
					new FastaRecordWritable(DNAString.stringToBytes(sequence.substring(offset, end)),offset));
//			FIXME: should we use BytesWritable instead?
			if (end == fulllength) 
			{ 
				offset = fulllength; 
			}
			else
			{
				offset = end - CHUNK_OVERLAP;
			}
		}		
		writer.sync();		
		if (numchunks > 1)
		{
			System.out.println("  " + numchunks + " chunks");
		}
	}
	public static void convertFile(String infile, SequenceFile.Writer writer) throws IOException
	{
		String header = "";
		StringBuilder sequence = null;			
		int count = 0;			
		try 
		{
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
						saveSequence(header, sequence, writer);
					}			
					sequence = new StringBuilder();
					header = line.substring(1); // skip the >
					count++;
				}
				else
				{
					sequence.append(line.toUpperCase());
					//FIXME: normally, the referent file is often very large, can the sequence (StringBuilder) handle it? 
				}
			}				
			saveSequence(header, sequence, writer);				
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
		String infile=null;
		String outfile=null;
		if (args.length == 2) {				
			infile = args[0];
			outfile = args[1];
		}
		else if (args.length==4){
			CHUNK_SIZE = Integer.parseInt(args[2]);
			CHUNK_OVERLAP = Integer.parseInt(args[3]);
		} else {
			System.err.println("Usage: ConvertReferenceFile file.fa outfile.br [chunk_size chunk_overlap]");
			System.exit(-1);
		}

		System.err.println("Converting " + infile + " into " + outfile);

		Configuration config = new Configuration();

		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(outfile), IntWritable.class, FastaRecordWritable.class);

		convertFile(infile, writer);

		writer.close();

		System.err.println("min_seq_len: " + min_seq_len);
		System.err.println("max_seq_len: " + max_seq_len);
		System.err.println("Using DNAString version: " + DNAString.VERSION);
	}
}
