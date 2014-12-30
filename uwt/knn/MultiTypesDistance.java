package uwt.knn;

import uwt.generic.Row;
import uwt.generic.Utility;

public class MultiTypesDistance implements DistanceFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public double getDistance(Row row1, Row row2) {
		// TODO Auto-generated method stub
		return Utility.getDistance(row1, row2)/row1.getNumOfAttributes();
	}

}
