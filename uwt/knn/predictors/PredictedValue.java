package uwt.knn.predictors;
import java.io.Serializable;

import uwt.knn.KnnRow;


public class PredictedValue implements Serializable {
	double actualValue;
	double predictedValue;
	double squaredError;
	int id;
	private int numberOfElementsCounted = 1;

	/*public PredictedCost(double trueCost, double predictedCost, int id) {
		super();
		this.trueCost = trueCost;
		this.predictedCost = predictedCost;
		this.squaredError = Math.pow(predictedCost - trueCost,2);
		this.id = id;
		numOfPredictionsInError = 1;
	}*/

	public PredictedValue(KnnRow row) {
		super();
		this.actualValue = (double)row.getClassValue();
		this.predictedValue = row.predict();
		this.squaredError = Math.pow(predictedValue - actualValue, 2);
		this.id = row.getId();
	}

	
	public double getActualValue() {
		return actualValue;
	}


	public void setActualValue(double actualValue) {
		this.actualValue = actualValue;
	}


	public double getPredictedValue() {
		return predictedValue;
	}


	public void setPredictedValue(double predictedValue) {
		this.predictedValue = predictedValue;
	}


	public double getSquaredError() {
		return squaredError;
	}


	public void setSquaredError(double squaredError) {
		this.squaredError = squaredError;
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}

	public void addSquaredError(double squaredError)
	{
		squaredError+= squaredError;
		numberOfElementsCounted ++;
	}
	
	public double getRmseForAccumulatedErrors()
	{
		return Math.sqrt(squaredError/numberOfElementsCounted);
	}
	

	public String toString()
	{
		return "ID: "+ id+", ActualValue="+actualValue+", PredictedValue="+predictedValue + ", Error="+squaredError+ ", numIncluded="+ numberOfElementsCounted;
	}
	
	

}
