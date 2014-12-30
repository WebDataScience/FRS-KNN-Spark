package uwt.knn;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import uwt.generic.Mergeable;

public class KnnMergableList implements Mergeable {

	List<Tuple2<Integer, KnnRow>> list;
	
	
	
	public KnnMergableList() {
		list = new ArrayList<Tuple2<Integer, KnnRow>>();
	}

	@Override
	public Mergeable merge(Mergeable obj) {
		KnnMergableList list2 = (KnnMergableList)obj;
		list.addAll(list2.getList());
		return this;
	}
	
	public void add(Tuple2<Integer, KnnRow> row)
	{
		list.add(row);
	}
	
	public List<Tuple2<Integer, KnnRow>> getList()
	{
		return list;
	}

}
