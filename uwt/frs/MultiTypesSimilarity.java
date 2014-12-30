package uwt.frs;

import uwt.generic.Row;
import uwt.generic.Utility;

public class MultiTypesSimilarity implements FuzzySimilarityFunction {

	@Override
	public double getSimilarity(Row row1, Row row2) {
		double result = 0, numOfAttrs = row1.getNumOfAttributes();
		result = Utility.getDistance(row1, row2);
		result = (numOfAttrs - result)/numOfAttrs;
		return result;
	}

	@Override
	public double getSimilarity(double[] row1, double[] row2) {
		double numDistance = 0; 
		/*for(int i=row1.length-1;i>=0;i--)
		{
			numDistance += Math.abs(row1[i] - row2[i]);
		}*/
		return (row1.length - numDistance)/row1.length;
	}

}
