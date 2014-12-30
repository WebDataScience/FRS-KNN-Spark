package uwt.knn;

import java.util.ArrayList;
import java.util.List;

import uwt.generic.Row;
import uwt.generic.RowsDescriptor;
import uwt.knn.predictors.KnnPredictor;

import com.google.common.collect.Lists;
//import com.google.common.collect.MinMaxPriorityQueue;
import uwt.generic.MinMaxPriorityQueue;

public class KnnRow extends Row{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int foldNo;

	MinMaxPriorityQueue<NearestNeighbor> knnList;
	int k;
	KnnPredictor predictor;
	Object classValue;
	
	double quality;

	public KnnRow(String line, RowsDescriptor rowFormat, int k,
			KnnPredictor predictor) {
		super(line, rowFormat);
		knnList = MinMaxPriorityQueue.maximumSize(k).create();
		this.k = k;
		this.foldNo = id % 10;
		this.predictor = predictor;
		predictor.setClassValue(this);
		// TODO Auto-generated constructor stub
	}
	
	public KnnRow(Row row, int k, KnnPredictor predictor) {
		this.line = row.getLine();
		this.id = row.getId();
		this.numericAttributes = row.getNumericAttributes();
		this.booleanAttributes = row.getBooleanAttributes();
		this.stringAttributes = row.getStringAttributes();
		this.numOfAttributes = row.getNumOfAttributes();
		this.label = row.getLabel();

		knnList = MinMaxPriorityQueue.maximumSize(k).create();
		this.k = k;
		this.foldNo = id % 10;
		this.predictor = predictor;
		predictor.setClassValue(this);
	}
	
	public void scale(double[] ranges)
	{
		super.scale(ranges);
	}

	public void addNearestNeighbor(NearestNeighbor nn) {
		/*if (knnList.size() < k)
			knnList.add(nn);
		else {
			if (knnList.get(k - 1).distance > nn.getDistance())
				knnList.set(k - 1, nn);
		}
		Collections.sort(knnList);*/
		knnList.add(nn);
	}

	public int getFoldNo() {
		return foldNo;
	}

	public void setFoldNo(int foldNo) {
		this.foldNo = foldNo;
	}

	public List<NearestNeighbor> getKnnList() {
		return Lists.newArrayList(knnList.iterator());
	}

	public void setKnnList(List<NearestNeighbor> knnList) {
		this.knnList = MinMaxPriorityQueue.maximumSize(k).create();
		this.knnList.addAll(knnList);
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public String toString() {
		return line;
	}

	public double predict() {
		return this.predictor.predict(this);
	}

	public Object getClassValue() {
		return classValue;
	}

	public void setClassValue(Object classValue) {
		this.classValue = classValue;
	}
	
	public String getKnnString()
	{
		String result = "";
		
		for(NearestNeighbor nn:knnList)
		{
			result+="[ID=+"+nn.getId()+",D="+nn.getDistance()+"], ";
		}
		return result;
	}
	
	public String checkRow()
	{
		return id+","+quality;
	}

	public double getQuality() {
		return quality;
	}

	public void setQuality(double quality) {
		this.quality = quality;
	}

}
