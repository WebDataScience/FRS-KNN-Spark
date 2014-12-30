package uwt.knn.predictors;

import java.util.Collections;
import java.util.List;

import uwt.knn.KnnRow;
import uwt.knn.NearestNeighbor;

public class KnnCostPredictor implements KnnPredictor {

	@Override
	public double predict(KnnRow row) {
		// TODO Auto-generated method stub
		return getWeightedAvgCost(row);
		//return avg(row);
	}
	
	public double avg(KnnRow row)
	{
		double predicted=0;
		List<NearestNeighbor> knnList = row.getKnnList();
		double weightedCost = 0;
		for(NearestNeighbor nn: knnList)
		{
			weightedCost += Double.parseDouble(nn.getLabel());
		}
		predicted = weightedCost/knnList.size();
		return weightedCost;
	}
	
	public double getWeightedAvgCost(KnnRow row)
	{
		double predicted=0;
		List<NearestNeighbor> knnList = row.getKnnList();
		
		if(knnList.isEmpty())
			predicted = Double.parseDouble(row.getLabel());
		else
		{
			Collections.sort(knnList, Collections.reverseOrder());
			double distanceRange = knnList.get(knnList.size()-1).getDistance() - knnList.get(0).getDistance();
			double largestDistance = knnList.get(knnList.size()-1).getDistance();
			double weightedCost = 0;
			double weight;
			double totalWeight = 0;
			
			if(distanceRange == 0)
			{
				for(NearestNeighbor nn: knnList)
				{
					weightedCost += Double.parseDouble(nn.getLabel());
				}
				predicted = weightedCost/knnList.size();
				
			}
			else
			{
				for(NearestNeighbor nn: knnList)
				{
					if(distanceRange == 0)
						weight = 1;
					else
					weight = (largestDistance - nn.getDistance())/distanceRange;
					totalWeight += weight;
					weightedCost += weight * Double.parseDouble(nn.getLabel());
				}
				predicted = weightedCost/totalWeight;
			}
		}
		return predicted;
	}

	@Override
	public void setClassValue(KnnRow row) {
		// TODO Auto-generated method stub
		row.setClassValue(Double.parseDouble(row.getLabel()));
	}

}
