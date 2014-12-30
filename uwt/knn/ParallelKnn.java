package uwt.knn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import uwt.generic.Row;
import uwt.generic.Utility;


public class ParallelKnn implements Callable<List<NearestNeighbor>> {
	int start,end;
	int id;
	Row[] protoTypeRows;
	Row testRow;
	int k;
	
	public ParallelKnn(int id, int start, int end, Row[] protoTypes, Row testRow, int k) {
		this.start = start;
		this.end = end;
		this.protoTypeRows = protoTypes;
		this.testRow = testRow;
		this.k = k;
		this.id = id;
		
	}

	public List<NearestNeighbor> runIteration(int start, int end) {
		Row protoTypeRow;
		double distance = 0;
		NearestNeighbor neighbour;
		ArrayList<NearestNeighbor> knnList = new ArrayList<NearestNeighbor>(k);
		
		for(int i=start;i<end;i++)
		{
			protoTypeRow = protoTypeRows[i];
			distance = Utility.getDistance(testRow,protoTypeRow);
			neighbour = new NearestNeighbor();
			neighbour.setId(protoTypeRow.getId());
			neighbour.setDistance(distance);
			neighbour.setLabel(protoTypeRow.getLabel());
			if(knnList.size()<k)
				knnList.add(neighbour);
			else{
				if(knnList.get(k-1).distance>distance)
					knnList.set(k-1, neighbour);
			}
			Collections.sort(knnList);
		}
		return knnList;
	}
	
	/*public double getDistance(Row row1, Row row2)
	{
		double[] numAttr1 = row1.getNumericAttributes();
		double[] numAttr2 = row2.getNumericAttributes();
		
		int[] boolAttr1 = row1.getBooleanAttributes();
		int[] boolAttr2 = row2.getBooleanAttributes();
		
		String[] strAttr1 = row1.getStringAttributes();
		String[] strAttr2 = row2.getStringAttributes();
		
		double numDistance = 0;
		for(int i=0;i<numAttr1.length;i++)
		{
			numDistance += Math.abs(numAttr1[i] - numAttr1[i]);
		}
		
		double boolDistance = 0;
		for(int i=0;i<boolAttr1.length;i++)
		{
			boolDistance += getJaccardSim(boolAttr1[i],boolAttr1[i]);
		}
		boolDistance = boolAttr1.length - boolDistance;
		
		double strDistance = 0;
		for(int i=0;i<boolAttr1.length;i++)
		{
			strDistance += strAttr1[i].equals(strAttr1[i])?1:0;
		}
		
		return (numDistance + boolDistance + strDistance)/row1.getNumOfAttributes();
	}
	
	//A set of attributes is represented by a 32bit number where each bit represents an attribute type.
	public double getJaccardSim(int set1,int set2)
	{
		int intersection = set1 & set2;
		int union = set1 | set2;
		if(union == 0) //if both sets are empty, return 0
			return 1;
		int jacardSim = Integer.bitCount(intersection) / Integer.bitCount(union);
		
		return jacardSim;
	}*/

	@Override
	public List<NearestNeighbor> call() throws Exception {
		// TODO Auto-generated method stub
		return runIteration(start, end);
	}

}
