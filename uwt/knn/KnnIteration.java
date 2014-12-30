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
import uwt.generic.Mergeable;
import uwt.generic.ParallelIteration;
import uwt.generic.Row;
import uwt.generic.RowsDescriptor;
import uwt.generic.Utility;
import uwt.knn.predictors.KnnPredictor;

public class KnnIteration extends ParallelIteration {

	private String[] lines;
	private RowsDescriptor rowFormat;
	private KnnRow[] inMemoryRows;
	private DistanceFunction dFunction;
	int k;
	KnnPredictor predictor;
	//List<Tuple2<Integer, KnnRow>> nnList;
	@Override
	public Mergeable runIteration() {
		KnnRow testRow;
		KnnMergableList nnList = new KnnMergableList();
		for(int j=start; j<end; j++)
		{
			if(lines[j] == null)
				break;

			testRow = new KnnRow(lines[j], rowFormat, k, predictor);
			Utility.setKnn(k, testRow, inMemoryRows);
			nnList.add(new Tuple2<Integer, KnnRow>(testRow.getId(),testRow));
		}
		return nnList;
	}

	@Override
	public void setParameters(Object parameters) {
		Map param = (Map)parameters;
		lines = (String[]) param.get("lines");
		rowFormat = (RowsDescriptor) param.get("rowFormat");
		dFunction = (DistanceFunction) param.get("dFunction");
		inMemoryRows = (KnnRow[]) param.get("inMemoryRows");
		k = (int)param.get("k");
		predictor = (KnnPredictor) param.get("predictor");
		//InMemoryRows inMemRows = (InMemoryRows) param.get("inMemoryRows");
		//inMemoryRows = inMemRows.getInMemRows();
	}
	
	public Object generateParameters(String[] lines,
			RowsDescriptor rowFormat,KnnRow[] inMemoryRows,
			DistanceFunction dFunction, KnnPredictor predictor, int k) {
		
		Map param = new HashMap();
		param.put("lines", lines);
		param.put("rowFormat", rowFormat);
		param.put("inMemoryRows", inMemoryRows);
		param.put("dFunction", dFunction);
		param.put("k", k);
		param.put("predictor", predictor);
		
		setParameters(param);
		return param;

	}
}
