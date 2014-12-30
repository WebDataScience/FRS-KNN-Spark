package uwt.knn;

import java.io.Serializable;

import uwt.generic.Row;

public interface DistanceFunction extends Serializable{

	public double getDistance(Row row1, Row row2);
}
