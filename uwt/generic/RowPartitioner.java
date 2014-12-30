package uwt.generic;
import org.apache.spark.Partitioner;


public class RowPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = -919683287550633263L;
	int numPartitions;
	
	public RowPartitioner(int p)
	{
		numPartitions = p;
	}
	@Override
	public int getPartition(Object arg0) {
		Integer row = (Integer)arg0;
		int result = 0;
		if(row!=null)
			result = row%numPartitions;
		return result;
	}

	@Override
	public int numPartitions() {
		return numPartitions;
	}
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}
	
	

}
