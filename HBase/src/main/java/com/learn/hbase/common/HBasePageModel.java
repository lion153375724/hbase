package com.learn.hbase.common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Description: HBase表数据分页模型类。<br>
 * 利用此类可管理多个HBaseQualifierModel对象。
 * @author jason
 * @createTime 2017年11月8日下午4:53:56
 */
public class HBasePageModel implements Serializable {
	private Integer currentPage;
    private Integer pageSize;
    private Integer totalCount;
    private Integer totalPage;
    private List<Map<String, String>> resultList;
    public Integer getCurrentPage() {
        return currentPage;
    }
    public void setCurrentPage(Integer currentPage) {
        this.currentPage = currentPage;
    }
    public Integer getPageSize() {
        return pageSize;
    }
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
    public Integer getTotalCount() {
        return totalCount;
    }
    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }
    public Integer getTotalPage() {
        return totalPage;
    }
    public void setTotalPage(Integer totalPage) {
        this.totalPage = totalPage;
    }
    public List<Map<String, String>> getResultList() {
        return resultList;
    }
    public void setResultList(List<Map<String, String>> resultList) {
        this.resultList = resultList;
    }
}
