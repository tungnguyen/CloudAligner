package common;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

public class Utils {
	public static final long low_bit = 1l;
	public static final long high_bit = 0x8000000000000000l;
	public static final long all_ones = -1l;
	public static final long all_zeros = 0l;
	public static final int word_size = 64;
	public static final int alphabet_size = 4;
	static final int INPUT_BUFFER_SIZE = 1000000;	
	
	static int base2int_upper_only(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 2;
		case 'T' : return 3;
		default  : return 4;
		}
	}

	public static int	base2int(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 2;
		case 'T' : return 3;
		case 'a' : return 0;
		case 'c' : return 1;
		case 'g' : return 2;
		case 't' : return 3; 
		default  : return 4;
		}
	}

	static int	base2int_bs_upper_only(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 3;
		case 'G' : return 2;
		case 'T' : return 3;
		default  : return 4;
		}
	}

	static int	base2int_bs(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 3;
		case 'G' : return 2;
		case 'T' : return 3;
		case 'a' : return 0;
		case 'c' : return 3;
		case 'g' : return 2;
		case 't' : return 3; 
		default  : return 4;
		}
	}

	static int	base2int_bs_ag_upper_only(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 0;
		case 'T' : return 3;
		default  : return 4;
		}
	}

	static int	base2int_bs_ag(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 0;
		case 'T' : return 3;
		case 'a' : return 0;
		case 'c' : return 1;
		case 'g' : return 0;
		case 't' : return 3; 
		default  : return 4;
		}
	}


	static int	base2int_bs_rc(char c) {
		switch(c) {
		case 'A' : return 0;
		case 'C' : return 1;
		case 'G' : return 0;
		case 'T' : return 3;
		case 'a' : return 0;
		case 'c' : return 1;
		case 'g' : return 0;
		case 't' : return 3; 
		}
		return 4;
	}

	static int	base2int_rc(char c) {
		switch(c) {
		case 'A' : return 3;
		case 'C' : return 2;
		case 'G' : return 1;
		case 'T' : return 0;
		case 'a' : return 3;
		case 'c' : return 2;
		case 'g' : return 1;
		case 't' : return 0; 
		}
		return 4;
	}

	static char	int2base(int c) {
		switch(c) {
		case 0 : return 'A';
		case 1 : return 'C';
		case 2 : return 'G';
		case 3 : return 'T';
		}
		return 'N';
	}

	static char	int2base_rc(int c) {
		switch(c) {
		case 3 : return 'A';
		case 2 : return 'C';
		case 1 : return 'G';
		case 0 : return 'T';
		}
		return 'N';
	}

	public static boolean	isvalid(char c) {
		return (base2int(c) != 4);
	}

	// Code for dealing with the DNA alphabet

	static char complement(int i) {
		final int b2c_size = 20;
		final char[] b2c = {
			//A,  b,  C,  d,  e,  f,  g,  h,  i,  j,  k,  l,  m,  n,  o,  p,  q,  r,  s,  T
			'T','N','G','N','N','N','C','N','N','N','N','N','N','N','N','N','N','N','N','A'
		};
		final char[] b2cl = {
			//A,  b,  C,  d,  e,  f,  g,  h,  i,  j,  k,  l,  m,  n,  o,  p,  q,  r,  s,  T
			't','n','g','n','n','n','c','n','n','n','n','n','n','n','n','n','n','n','n','a'
		};
		if (i - 'A' >= 0 && i - 'A' < b2c_size)
			return b2c[i - 'A'];
		else if (i - 'a' >= 0 && i - 'a' < b2c_size)
			return b2cl[i - 'a'];
		else return 'N';
	}

	static String	revcomp(String s) {		
		int len = s.length();
		char re[]=new char[len];		
		for (int i=0;i<len;i++)
			re[i] = complement(s.charAt(i));
		String r = new String(re);
		StringBuffer buf = new StringBuffer(r);
		buf = buf.reverse();		
		return buf.toString();
	}

	static String revcomp_inplace(String s) {
		int len = s.length();
		char re[]=new char[len];		
		for (int i=0;i<len;i++)
			re[i] = complement(s.charAt(i));
		String r = new String(re);
		StringBuffer buf = new StringBuffer(r);
		return buf.reverse().toString();		
	}

	public static String bits2string_masked(long mask, long bits) {
		String s="";
		long selector = high_bit;
		for (int i = 0; i < word_size; ++i) {
			s += ((selector & bits & mask)!=0) ? '1' : '0';
			selector >>>= 1;
		}
		return s;
	}

	static String  bits2string_for_positions(int positions, long bits) {
		String s="";
		long selector = high_bit;
		for (int i = 0; i < word_size; ++i) {
			s += ((selector & bits)!=0) ? '1' : '0';
			selector >>>= 1;
		}
		return s.substring(s.length() - positions);
	}


	
	public static void read_fasta_file(String filename, 
			Vector<String> names, Vector<String> sequences) {

		BufferedReader inputStream = null;

		try {
			inputStream = 
				new BufferedReader(new FileReader(filename));
			boolean first_line = true;
			String s="",l,name="";
			while ((l = inputStream.readLine()) != null) {
				//System.out.println(l);
				if (l.charAt(0) == '>') {
					if (first_line == false && l.length() > 0) {
						names.add(name);
						sequences.add(s);						
					}
					else first_line = false;
					name = l.substring(1);	
					s = "";
				}else
					s+=l;
			}
			if (!first_line && s.length() > 0) {
				names.add(name);
				sequences.add(s);
			}
		} catch(IOException ioe)
		{
			System.out.println("Error while closing the stream : " + ioe);
		}finally {
			try{
				if (inputStream != null) {
					inputStream.close();
				}  
			}catch(IOException ioe)
			{
				System.out.println("Error while closing the stream : " + ioe);
			}
		}	  
	}
	public static void read_fasta_file1(String filename, 
			Vector<String> names, Vector<byte[]> sequences) {
		 try {
		        int len = (int)(new File(filename).length());
		        FileInputStream fis =
		            new FileInputStream(filename);
		        byte buf[] = new byte[len];
		        fis.read(buf);
		        fis.close();
		        byte buf2[] = new byte[len];;
		        CharArrayWriter name = new CharArrayWriter();
		        boolean firstline = true;
		        int i=0,j=0;
		        while (i<len){		        
		        	if (buf[i]=='>'){
		        		buf2 = new byte[len];//more than we need
		        		j=0;
		        		name = new CharArrayWriter();
		        		while ((buf[++i]!='\n')&&(i<len)){ //skip the info line	        			
		        			name.append((char)buf[i]);
		        		}
		        		if (firstline) firstline = false;
		        		else {
		        			names.add(name.toString());
		        			sequences.add(buf2);
		        		}
		        	} else if (buf[i]!='\n')
		        	buf2[j++]=buf[i];
		        	i++;
		        }		
		        names.add(name.toString());
    			sequences.add(buf2);
		      }
		      catch (IOException e) {
		        System.err.println(e);
		      }
	}
	public static void read_filename_file(String filename, Vector<String> filenames) {
		BufferedReader inputStream = null;
		try {
			inputStream = 
				new BufferedReader(new FileReader(filename));

			String l;
			while((l = inputStream.readLine()) != null) {
				System.out.println(l);
				filenames.add(l);
			}
		} catch(IOException ioe)
		{
			System.out.println("Error while closing the stream : " + ioe);
		}finally {
			try{
				if (inputStream != null) {
					inputStream.close();
				}  
			}catch(IOException ioe)
			{
				System.out.println("Error while closing the stream : " + ioe);
			}
		}	 
	}
	
	public static boolean	is_valid_filename(String name, String filename_suffix) {
		int dot = name.lastIndexOf('.');
		final String suffix = name.substring(dot + 1);	  
		return (suffix == filename_suffix);
	}
	
	public static void read_dir(String dirname, String filename_suffix,
			Vector<String> filenames) {
		File directory = new File(dirname);
		String filenamelist[] = directory.list();

		for (int i = 0; i < filenamelist.length; i++) {
			if (!is_valid_filename(filenamelist[i], filename_suffix))
				filenames.add(filenamelist[i]);
		}
		if (filenames.isEmpty())
			System.out.println("no valid files found in: " + dirname);
	}
}
