package uwt.generic;

import java.io.Serializable;

public class MinMax implements Serializable {
	double[] min,max;

	public MinMax(double[] min, double[] max) {
		super();
		this.min = min;
		this.max = max;
	}

	public double[] getMin() {
		return min;
	}

	public void setMin(double[] min) {
		this.min = min;
	}

	public double[] getMax() {
		return max;
	}

	public void setMax(double[] max) {
		this.max = max;
	}
	
	

}
