package uwt.generic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class MyLogger implements Serializable {
	PrintWriter outFile = null;

    
	public MyLogger(String name) throws IOException {
		outFile = new PrintWriter(new FileOutputStream(new File(name), true));
	}
	
	public void log(String msg)
	{
		Date d = new Date();
		outFile.println(d+": "+msg);
		outFile.flush();
	}
	
	public void close()
	{
		outFile.close();
	}

	

}
