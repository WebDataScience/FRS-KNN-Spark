package uwt.knn;

import uwt.generic.Row;
import uwt.generic.Utility;

/**
 * A class that is used to compute the distance between two attributes. The attributes can numeric, string or boolean.
 * @author Hasan
 *
 */
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
