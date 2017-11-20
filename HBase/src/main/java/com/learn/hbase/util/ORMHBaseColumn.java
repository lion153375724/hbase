package com.learn.hbase.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * 榛樿id涓簉owkey.濡傛灉id涓嶆槸rowkey,褰撹浆鎹ut鏃�,浼氭姤閿�.
 * 榛樿鎵�鏈夌殑瀛楁绫诲瀷涓篠tring.濡備笉鏄疭tring.杞崲浼氶敊璇�.
 * 鍙互琛ュ厖HBaseUtil鐨勮浆鎹�.
 * @author gaoweigong
 * @createTime 2016-01-28
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD })
@Inherited
public @interface ORMHBaseColumn {
	public String family() default "";
	public String qualifier() default "";
	public boolean timestamp() default false;
}
