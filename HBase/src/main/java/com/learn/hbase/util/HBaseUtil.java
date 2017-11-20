package com.learn.hbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.impl.cookie.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * bena转put,result工具类
 * 
 * @author gaoweigong
 * @createTime 2016-02-14
 */
public class HBaseUtil {
	public static Logger LOGGER = LoggerFactory.getLogger(HBaseUtil.class);

	/**
	 * JavaBean转换为Put
	 * @param <T>
	 * 
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static <T> Put bean2Put(T obj) throws Exception {
		Put put = new Put(Bytes.toBytes(parseObjId(obj)));
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (!field.isAnnotationPresent(ORMHBaseColumn.class)) {
				continue;
			}
			field.setAccessible(true);
			ORMHBaseColumn orm = field.getAnnotation(ORMHBaseColumn.class);
			String f = orm.family();
			String q = orm.qualifier();
			if (StringUtils.isBlank(f) || StringUtils.isBlank(q)) {
				continue;
			}
			Object fieldObj = field.get(obj);
			//TODO package value
			if (fieldObj.getClass().isArray()) {
				LOGGER.error("nonsupport");
			}
			if (q.equalsIgnoreCase("rowkey") || f.equalsIgnoreCase("rowkey")) {
				continue;
			} else {
				if (field.get(obj) != null || StringUtils.isNotBlank(field.get(obj).toString())) {
					put.addColumn(Bytes.toBytes(f), Bytes.toBytes(q), Bytes.toBytes(field.get(obj).toString()));
				}
			}
		}
		return put;
	}

	/**
	 * 获取Bean中的id,作为Rowkey
	 * @param <T>
	 * 
	 * @param obj
	 * @return
	 */
	public static <T> String parseObjId(T obj) {
		Class<?> clazz = obj.getClass();
		try {
			Field field = clazz.getDeclaredField("id");
			field.setAccessible(true);
			Object object = field.get(obj);
			return object.toString();
		} catch (NoSuchFieldException e) {
			//如果没有id字段,则设置rowkey字段作为rowkey
			try {
				Field field = clazz.getDeclaredField("rowKey");
				field.setAccessible(true);
				Object object = field.get(obj);
				return object.toString();
			} catch (Exception e2) {
				LOGGER.warn("", e2);
			}
			LOGGER.warn("", e);
		} catch (SecurityException e) {
			LOGGER.warn("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.warn("", e);
		} catch (IllegalAccessException e) {
			LOGGER.warn("", e);
		}
		return "";
	}
	/**
	 * HBase result 转换为 bean
	 * @param <T>
	 * @param result
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static <T> T result2Bean(Result result, T obj) throws Exception {
		if (result == null) {
			return null;
		}
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (!field.isAnnotationPresent(ORMHBaseColumn.class)) {
				continue;
			}
			ORMHBaseColumn orm = field.getAnnotation(ORMHBaseColumn.class);
			String f = orm.family();
			String q = orm.qualifier();
			boolean timeStamp = orm.timestamp();
			if (StringUtils.isBlank(f) || StringUtils.isBlank(q)) {
				continue;
			}
			String fieldName = field.getName();
			String value = "";
			if (f.equalsIgnoreCase("rowkey")) {
				value = new String(result.getRow());
			} else {
				value =getResultValueByType(result, f, q, timeStamp);
			}
			String firstLetter = fieldName.substring(0, 1).toUpperCase();
			String setMethodName = "set" + firstLetter + fieldName.substring(1);
			Class<?> type = field.getType();
			Method setMethod = clazz.getMethod(setMethodName, type);
			if(null != value && value.length() > 0)
			if(type == Date.class){
				setMethod.invoke(obj, StringToDate(value));
			}else if(type == String.class){
				setMethod.invoke(obj, value);
			}
		}
		return obj;
	}
	
	private static Date StringToDate(String dateStr){
		SimpleDateFormat sfStart = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",java.util.Locale.ENGLISH) ;
		try {
			return sfStart.parse(dateStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * TODO add other type value
	 * @param result
	 * @param family
	 * @param qualifier
	 * @param timeStamp
	 * @return
	 */
	private static String getResultValueByType(Result result, String family, String qualifier, boolean timeStamp) {
		/*String result1 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
		if(result1 != null){
			if (!timeStamp) {
				return result1.toString();
			}
			List<Cell> cells = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			if (cells.size() == 1) {
				Cell cell = cells.get(0);
				return cell.getTimestamp() + "";
			}
		}
		return "";*/
		return Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
	}

}
