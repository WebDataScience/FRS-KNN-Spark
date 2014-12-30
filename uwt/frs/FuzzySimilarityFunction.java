package uwt.frs;

import java.io.Serializable;

import uwt.generic.Row;

public interface FuzzySimilarityFunction extends Serializable{
	
	public double getSimilarity(Row row1, Row row2);
	public double getSimilarity(double[] row1, double[] row2);

}
