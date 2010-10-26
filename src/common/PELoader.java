package common;

import java.util.Vector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class PELoader {
	private static int read_width = 0;
	static char	to_base_symbol(char c) {return (Utils.isvalid(c)) ? Character.toUpperCase(c) : 'N';}

	static long	make_read_word(char[] s) {
		long multiplier = 1l; 
		long index = 0l;
		int n = s.length;
		do { 
			--n;
			index += (long)Utils.base2int(s[n])*multiplier;
			multiplier *= Utils.alphabet_size; 
		} while (n > 0);
		return index;
	}
	static long	get_read_word(char[] read) {
		final int trunc_to = Math.min(read.length, SeedMaker.max_seed_part);
		// Need to replace the Ns because otherwise they will destroy the
		// conversion from DNA to integers. Could replace with random
		// bases, but everyone hates non-deterministic programs.
		char[] tmp = new char[trunc_to];
		for (int i=read.length - trunc_to;i<read.length;i++){
			tmp[i] = (read[i]=='N')?'A':read[i];
		}
		return make_read_word(tmp);
	}
	int count(char[] s, char c){
		int counter=0;
		for (int i=0;i<s.length;i++){
			if (s[i]==c) counter++;
		}
		return counter;
	}
	void check_and_add(byte[] read, byte[] qual, final int max_diffs,
			Vector<FastRead> fast_reads_left, Vector<FastRead> fast_reads_right, 
			Vector<Long> read_words, Vector<Integer> read_index,
			Vector<byte[]> orireads,Vector<byte[]> qualities,			
			int read_count) throws SMAPException {
//		System.out.println("read length"+read.length);		
		int half_width = read.length/2;
		if (read_width == 0) {
			if (half_width*2 != read.length)
				throw new SMAPException("PE reads not even length (must specify length)");
			read_width = read.length/2;
		}
		else if (read.length < 2*read_width)
			throw new SMAPException("Incorrect read width");		
//		System.out.println("half width"+half_width);
//		System.out.println("read width"+read_width);
		if (read_count == 0)
			FastRead.set_read_width(read_width);

		// clean the read
		char[] tmpstr = new char[read.length];
		assert(tmpstr.length==read.length);
		char[] right_read = new char[read_width];
		char[] left_read = new char[read_width];
		
		for (int i=0;i<read.length;i++){
			tmpstr[i] = to_base_symbol((char)read[i]);
			if (i<read_width)
				left_read[i] = tmpstr[i];
			else
				right_read[i-half_width] = tmpstr[i];	
		}
		String right_string = new String(right_read);
		// check for quality
		final boolean good_read = (count(tmpstr, 'N') <= 2*max_diffs);
		if (good_read) {
//			System.out.println("right string"+right_string);
//			System.out.println("left string"+new String(left_read));
//			System.out.println("whole string"+ new String(tmpstr));
			fast_reads_left.add(new FastRead(new String(left_read)));			
			String revstr = Utils.revcomp_inplace(right_string);
			fast_reads_right.add(new FastRead(revstr));
			read_words.add(get_read_word(revstr.toCharArray()));
			orireads.add(read);//add here instead of in the load_reads_from.. functions because we want to check the quality of read before add them in
			if (qual!=null) qualities.add(qual);
			read_index.add(read_count);
		} 
		
	}
	public int load_reads_from_fasta_file(SequenceFile.Reader theReader, final int max_diffs,
			int readlength, final int maxReadPerRound,
			Vector<FastRead> fast_reads_left, Vector<FastRead> fast_reads_right,
			Vector<Long> read_words, Vector<Integer> read_index, Vector<String> read_names,
			Vector<byte[]> orireads,Vector<byte[]> qualities)//this is used to create SAM output file  
	{
		if (theReader==null) System.out.println("load_reads_from_fasta_file:null reader");
		try{			
			int numrecords = 0;
			read_width = readlength;
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			BytesWritable value = new BytesWritable();
//			boolean hasnext = false;
			while((numrecords < maxReadPerRound) && (theReader.next(key,value)))
			{	
				byte [] seq = new byte[value.getLength()]; 
				System.arraycopy(value.getBytes(), 0, seq, 0, value.getLength());
				//This is because the trap in Hadoop implementation which returns a longer seq than we expect
//				String s = DNAString.bytesToString(seq);
//				System.out.println(s);			
				check_and_add(seq, null, max_diffs, fast_reads_left, fast_reads_right,
						read_words, read_index, orireads, qualities, numrecords);
				read_names.add(key.toString());
//				if (Integer.parseInt(key.toString())>190) 
//				System.out.println("read seen: "+key.toString());
				//System.out.println(value.m_name.toString());
				numrecords++;
				if (theReader.syncSeen()){					
//					System.out.println("before seeing sync:"+theReader.getPosition());
//					System.out.println("key before seeing sync:"+key.toString());
					theReader.sync(theReader.getPosition()+10);	
//					System.out.println("key after seeing sync:"+key.toString());
//					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return numrecords;//eof
				}
			}			
			return numrecords;
		} catch (Exception e){
//			if (!value.getClass().equals(BytesWritable.class))
			System.err.println("The input read file may not be in the fasta format");
			e.printStackTrace();
		}		
		return 0;
	}
	/*
	public static void load_reads_from_fasta_file1(FSDataInputStream inputStream, final int max_diffs,
			Vector<FastRead> fast_reads,
			Vector<Integer> read_words, Vector<Integer> read_index) throws SMAPException {
		BufferedReader d = null;
		int read_count = 0;
		try {
			d = new BufferedReader(new InputStreamReader(inputStream));
			boolean first_line = true;
			String s="",l;
			while ((l = d.readLine()) != null) {
				//System.out.println(l);				
				if (l.charAt(0) == '>') {
					if (first_line == false && l.length() > 0) {
						check_and_add(s, max_diffs, fast_reads, 
								read_words, read_index, read_count);
						++read_count;
					}
					else first_line = false;
					//name = l.substring(1);	
					s = "";
				}else
					s+=l;
			}
			if (!first_line && s.length() > 0) {
				check_and_add(s, max_diffs, fast_reads, 
						read_words, read_index, read_count);
				++read_count;
			}
			if (fast_reads.isEmpty())
				throw new SMAPException("no high-quality reads in reads data");
		} catch(IOException ioe)
		{
			System.out.println("Error while closing the stream : " + ioe);
		}finally {
			try{
				if (d != null) {
					d.close();
				}  
			}catch(IOException ioe)
			{
				System.out.println("Error while closing the stream : " + ioe);
			}
		}	  
	}*/
	public int load_reads_from_fastq_file(SequenceFile.Reader theReader, final int max_diffs,
			int readlength, final int maxReadPerRound,
			Vector<FastRead> fast_reads_left, Vector<FastRead> fast_reads_right,
			Vector<Long> read_words, Vector<Integer> read_index,  Vector<String> read_names,
			Vector<byte[]> orireads,Vector<byte[]> qualities)//this is used to create SAM output file  
	{
		// in mismatch mode, this method only extracts the "read lines" in the fastq file
		if (theReader==null) System.out.println("load_reads_from_fasta_file:null reader");
		int numrecords = 0;
		try{			
			read_width = readlength;
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			ReadRecordWritable value = new ReadRecordWritable();
//			boolean hasnext = false;
			while((numrecords < maxReadPerRound) && (theReader.next(key,value)))
			{	
				byte [] seq = new byte[value.m_sequence.getLength()]; 
				byte [] qual = new byte[value.m_qsequence.getLength()]; 
				System.arraycopy(value.m_sequence.getBytes(), 0, seq, 0, value.m_sequence.getLength());
				System.arraycopy(value.m_qsequence.getBytes(), 0, qual, 0, value.m_qsequence.getLength());
//				This is because the trap in Hadoop implementation which returns a longer seq than we expect
//				String s = DNAString.bytesToString(seq);
//				System.out.println(s);			
				check_and_add(seq, qual, max_diffs, fast_reads_left, fast_reads_right, 
						read_words, read_index, orireads, qualities, numrecords);
				read_names.add(key.toString());
//				if (Integer.parseInt(key.toString())>190) 
//				System.out.println("read seen: "+key.toString());
				//System.out.println(value.m_name.toString());
				numrecords++;
				if (theReader.syncSeen()){					
//					System.out.println("before seeing sync:"+theReader.getPosition());
//					System.out.println("key before seeing sync:"+key.toString());
					theReader.sync(theReader.getPosition()+10);	
//					System.out.println("key after seeing sync:"+key.toString());
//					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return numrecords;//eof
				}
			}			
		} catch(Exception e)
		{
//			if (!value.getClass().equals(ReadRecordWritable.class))
			System.err.println("The input read file may not be in the fastq format");
			e.printStackTrace();
		}
		return numrecords;
	}
}
