package common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public abstract class Word {
	public abstract void shift(int i);
	public abstract int score(Word otherR, long score_mask);
	public abstract int score_ag(Word otherR, long score_mask);
	public abstract int score_tc(Word otherR, long score_mask);
	public Object copy(){
		Object obj = null;
        try {
            // Write the object out to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bos.toByteArray()));
            obj = in.readObject();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return obj;
    }
}
