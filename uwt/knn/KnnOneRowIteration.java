package uwt.knn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import uwt.frs.ApproxRow;
import uwt.frs.ClassVectorsGenerator;
import uwt.frs.FuzzySimilarityFunction;
import uwt.frs.InMemoryRows;
import uwt.generic.MergableList;
import uwt.generic.Mergeable;
import uwt.generic.MinMaxPriorityQueue;
import uwt.generic.ParallelIteration;
import uwt.generic.Row;
import uwt.generic.RowsDescriptor;
import uwt.generic.Utility;
import uwt.knn.predictors.KnnPredictor;

public class KnnOneRowIteration extends ParallelIteration {

	private KnnRow[] inMemoryRows;
	private KnnRow testRow;
	int k=0;
	@Override
	public Mergeable runIteration() {
		MergableList nnList = new MergableList();
		List<NearestNeighbor> knnList = Utility.computeKnn(k, testRow, inMemoryRows,start,end);
		nnList.add(knnList);
		
		return nnList;
	}

	@Override
	public void setParameters(Object parameters) {
		Map param = (Map)parameters;
		inMemoryRows = (KnnRow[]) param.get("inMemoryRows");
		k = (int)param.get("k");
		testRow = (KnnRow) param.get("testRow");
		//InMemoryRows inMemRows = (InMemoryRows) param.get("inMemoryRows");
		//inMemoryRows = inMemRows.getInMemRows();
	}
	
	public Object generateParameters(KnnRow testRow, KnnRow[] inMemoryRows, int k) {
		
		Map param = new HashMap();
		param.put("inMemoryRows", inMemoryRows);
		param.put("k", k);
		param.put("testRow", testRow);
		setParameters(param);
		return param;

	}
}
