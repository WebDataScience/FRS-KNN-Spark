package uwt.generic;

import java.util.ArrayList;
import java.util.List;

public class MergableList implements Mergeable {

	List list;
	
	public MergableList() {
		list = new ArrayList();
	}
	@Override
	public Mergeable merge(Mergeable obj) {
		MergableList list2 = (MergableList)obj;
		list.addAll(list2.getList());
		return this;
	}
	public List getList() {
		return list;
	}
	
	public void add(List l)
	{
		list.addAll(l);
	}
	
	

}
