package uwt.knn.predictors;

import java.io.Serializable;

import uwt.knn.KnnRow;

public interface KnnPredictor extends Serializable {

	public abstract double predict(KnnRow row);
	public abstract void setClassValue(KnnRow row);
}
