package uwt.knn.predictors;

import java.io.Serializable;

public class ErrorAccumulation implements Serializable{
	double error;
	int numOfElements;
	
	public ErrorAccumulation(double error, int numOfElements) {
		super();
		this.error = error;
		this.numOfElements = numOfElements;
	}
	
	public double getError() {
		return error;
	}
	public void setError(double error) {
		this.error = error;
	}
	public int getNumOfElements() {
		return numOfElements;
	}
	public void setNumOfElements(int numOfElements) {
		this.numOfElements = numOfElements;
	}
	
	public double getRmseForAccumulatedErrors()
	{
		return Math.sqrt(error/numOfElements);
	}
	
	

}
