package com.learn.hbase.common;

import java.util.List;

public class PageModel<T> {
	// 结果集
	private List<T> datas;
	// 当前第几页
	private int pageNo = 1;
	// 每页多少条记录
	private int pageSize = 20;
	// 总记录数
	private int rowCount;
	

	// 总页数
	public int getTotalPages() {
		return rowCount % pageSize == 0 ? rowCount / pageSize : rowCount
				/ pageSize + 1;
	}

	public List<T> getDatas() {
		return datas;
	}

	public void setDatas(List<T> datas) {
		this.datas = datas;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

}
