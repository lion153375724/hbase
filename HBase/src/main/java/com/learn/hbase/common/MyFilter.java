package com.learn.hbase.common;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;

/** 
 * 功能描述:自定义过滤器
 * @author : 
 * @date 创建时间：2017年3月30日 上午9:49:14 
 * @version 1.0  
 */
public class MyFilter extends FilterBase {

	private byte[] value = null;
	private boolean rowFilter = true;
	
	public MyFilter(Byte[] value,boolean rowFilter){
		super();
	}
	
	public MyFilter(byte[] value){
		this.value = value;
	}
	
	public void rest() throws Exception{
		this.rowFilter = true;
	}
	
	public boolean filterRow() throws IOException{
		return rowFilter;
	}
	
	@Override
	public ReturnCode filterKeyValue(Cell paramCell) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
