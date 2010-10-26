package common;

public class SMAPException extends Exception {
	private static final long serialVersionUID = 1L;
	String msg;
	public SMAPException()
	{
	}
	public SMAPException(String str)
	{
		msg = str;
	}
	public String toString(){
		return msg;
	}
}
