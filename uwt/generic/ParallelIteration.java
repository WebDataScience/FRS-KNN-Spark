package uwt.generic;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

public abstract class ParallelIteration implements Callable<Mergeable>, Cloneable {

	
	protected int start;
	protected int end;
	protected int id;
	
	public ParallelIteration() {

	}

	public abstract Mergeable runIteration();
	
	@Override
	public Mergeable call() throws Exception {
		
		return runIteration();
	}
	
	public void init(int start, int end) {
		UUID idOne = UUID.randomUUID();
        String str=""+idOne;        
        int uid=str.hashCode();
        String filterStr=""+uid;
        str=filterStr.replaceAll("-", "");
        this.id = Integer.parseInt(str);
        
		this.start = start;
		this.end = end;
	}
	
	public abstract void setParameters(Object parameters);
	
	public Object clone(){  
	    try{  
	        return super.clone();  
	    }catch(Exception e){ 
	        return null; 
	    }
	}

}




