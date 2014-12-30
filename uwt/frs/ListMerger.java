package uwt.frs;

import java.util.ArrayList;
import java.util.List;

import uwt.generic.Mergeable;

public class ListMerger implements Mergeable {
	List list = new ArrayList();
	@Override
	public Mergeable merge(Mergeable obj) {
		ListMerger m = (ListMerger)obj;
		list.addAll(m.getList());
		return null;
	}
	
	public List getList()
	{
		return list;
	}
	
	public void add(Object obj)
	{
		list.add(obj);
	}

}
