package com.bigdata.datacenter.datasync.utils

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.log4j.Logger

import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
 * 日期处理工具。
 * <p>
 * 相关的日期处理工具类为org.apache.commons.lang.time.DateFormatUtils
 * 
 * @since 1.0.0
 */

public class DateUtils {

	/** YYY格式 */
	public static final String FORMAT_DATE_YEAR = "yyyy"

	/** YYYY-MM 格式 */
	public static final String FORMAT_DATE_YEAR_MONTH = "yyyy-MM"

	/** YYYY-MM-dd 格式 */
	public static final String FORMAT_DATE = "yyyy-MM-dd"

	/** 默认格式 */
	public static final String FORMAT_TIME = "HH:mm:ss"

	/** 默认日期时间格式 */
	public static final String FORMAT_DATETIME = "yyyy-MM-dd HH:mm:ss"

	private static final Logger logger = Logger.getLogger(DateUtils.class)


	/**
	 * 将日期转换成字符格式
	 *
	 * @param date
	 *            java.util.Date类型
	 * @param format
	 *            如果为null或""，默认为DATE格式
	 * @return 无法成功转换则返回null
	 */
	public static String date2String(Date date, String format) {
		String result = null;
		if (date == null) {
			return result;
		}
		if (StringUtils.isEmpty(format)) {
			format = FORMAT_DATE;
		}
		try {
			result = DateFormatUtils.format(date, format);
		} catch (Exception ex) {
			logger.warn("日期转换为字符串错误，日期：" + date.toString() + "， 格式：" + format);
		}

		return result;
	}

	/**
	 * 将字符串转换成日期格式
	 *
	 * @param str
	 *            需要转换的字符串
	 * @param format
	 *            相应的转换格式，如果参数为Blank则表示按常规的三种格式转换
	 * @return 如果不能正常转换则返回null
	 */
	public static Date string2Date(String str, String format) {
		if (StringUtils.isEmpty(str)) {
			return null;
		}
		Date result = null;
		String[] formats = null;
		if (StringUtils.isEmpty(format)) {
			formats = new String[3];
			formats[0] = FORMAT_DATETIME;
			formats[1] = FORMAT_DATE;
			formats[2] = FORMAT_TIME;
		} else {
			formats = new String[4];
			formats[0] = format;
			formats[1] = FORMAT_DATETIME;
			formats[2] = FORMAT_DATE;
			formats[3] = FORMAT_TIME;
		}
		try {
			result = org.apache.commons.lang.time.DateUtils.parseDate(str, formats);
		} catch (Exception ex) {
			logger.warn("日期或时间格式不正确，日期时间字符串：" + str + "， 格式：" + format);

		}

		return result;
	}

	/**
	 * 将字符串转换成sql日期格式
	 *
	 * @param str
	 *            需要转换的字符串
	 * @param format
	 *            相应的转换格式，如果参数为Blank则表示按常规的三种格式转换
	 * @return 如果不能正常转换则返回null
	 */
	public static java.sql.Date string2SqlDate(String str, String format) {
		return new java.sql.Date(string2Date(str, format).getTime());
	}

	/**
	 * 将时间戳转换成字符串格式
	 *
	 * @param ts
	 *            时间戳
	 * @param format
	 *            日期时间格式
	 * @return 转换后的字符串
	 */
	public static String timestamp2String(Timestamp ts, String format) {
		return ts == null ? null : date2String(
				new Date(ts.getTime()), format);
	}

	/**
	 * 将字符串转换成时间戳格式
	 *
	 * @param str
	 *            需要转换的字符串
	 * @param format
	 *            相应的转换格式，如果参数为Blank则表示按常规的三种格式转换
	 * @return 如果不能正常转换则返回null
	 * @throws Exception
	 */
	public static Timestamp string2Timestamp(String str, String format) {
		return string2Date(str, format) == null ? null : new Timestamp(
				string2Date(str, format).getTime());
	}

	/**
	 * 添加年。
	 *
	 * @param date
	 *            日期
	 * @param num
	 *            添加的年数
	 * @return 添加后的日期
	 */
	public static Date addYears(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.YEAR, num);
		return cal.getTime();
	}

	/**
	 * 添加月份。
	 *
	 * @param date
	 *            日期
	 * @param num
	 *            添加对月数
	 * @return 添加后的日期
	 */
	public static Date addMonths(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MONTH, num);
		return cal.getTime();
	}

	/**
	 * 添加天数。
	 *
	 * @param date
	 *            日期
	 * @param num
	 *            添加的天数
	 * @return 添加后的日期
	 */
	public static Date addDays(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_YEAR, num);
		return cal.getTime();
	}

	/**
	 * 添加小时。
	 * @param date 日期
	 * @param num 添加的小时数
	 * @return 添加后的日期
	 */
	public static Date addHours(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, num);
		return cal.getTime();
	}

	/**
	 * 添加分钟
	 * @param date 日期
	 * @param num 添加的分钟数
	 * @return 添加后的日期
	 */
	public static Date addMinutes(Date date, int num) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MINUTE, num);
		return cal.getTime();
	}

	/**
	 * 得到当年第一天的开始时间。
	 *
	 * @param date
	 *            日期
	 * @return 当年第一天的开始时间
	 */
	public static Date getFirstDateOfYear(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.DAY_OF_YEAR, cal
				.getActualMinimum(Calendar.DAY_OF_YEAR));
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}

	/**
	 * 得到当月第一天的开始时间。
	 *
	 * @param date
	 *            日期
	 * @return 当月第一天的开始时间
	 */
	public static Date getFirstDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance()
		cal.setTime(date)
		cal.set(Calendar.DAY_OF_MONTH, cal
				.getActualMinimum(Calendar.DAY_OF_MONTH))
		cal.set(Calendar.HOUR_OF_DAY, 0)
		cal.set(Calendar.MINUTE, 0)
		cal.set(Calendar.SECOND, 0)
		return cal.getTime()
	}

	/**
	 * 得到当年的最后一天最后一秒。
	 *
	 * @param date
	 *            日期
	 * @return 当年最后一天最后一秒
	 */
	public static Date getLastDateOfYear(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.DAY_OF_YEAR, cal
				.getActualMaximum(Calendar.DAY_OF_YEAR));
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		return cal.getTime();
	}

	/**
	 * 得到当月的最后一天最后一秒。
	 *
	 * @param date
	 *            日期
	 * @return 当月最后一天最后一秒
	 */
	public static Date getLastDateOfMonth(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.DAY_OF_MONTH, cal
				.getActualMaximum(Calendar.DAY_OF_MONTH));
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		return cal.getTime();
	}

	/**
	 * 获得当前的星期的字符串形式，如：星期日，星期一……
	 * 
	 * @param date
	 *            日期
	 * @return 星期*
	 */
	public static String getStringDayOfWeek(Date date) {
		String result = "";
		if (null != date) {
			SimpleDateFormat formatter4 = new SimpleDateFormat("E");
			result = formatter4.format(date);
		}
		return result;
	}

	/**
	 * 获得两个日期的大小
	 * 
	 * @param date
	 *            日期
	 * @return 1 d1大于d2 0 d1等于d2 -1 d1小于d2
	 */
	public static int compareDate(Date d1, Date d2) {
		return d1.getTime() > d2.getTime() ? 1
				: (d1.getTime() < d2.getTime() ? -1 : 0);
	}

	/**
	 * 比较两个日，此方法用在天气解析日期用
	 * 
	 * @param day1
	 * @param day2
	 * @return -1 day1为day2之前 0 day1,day2是同一天 1 day1为day2之后
	 */
	public static int compareDay(int day1, int day2) {
		if (day1 == day2) {
			return 0;
		}
		if (Math.abs(day1 - day2) > 15) {
			if (day1 > day2) {
				return -1;
			} else {
				return 1;
			}
		} else if (day1 < day2) {
			return -1;
		} else {
			return 1;
		}
	}

	/**
	 * 比较两个日，此方法用在天气解析日期用
	 * 
	 * @param day1
	 * 
	 * @param day2
	 * 
	 * @return -2 day1day2 -1 day1day2 0 day1,day2 1 day1day2
	 */
	public static int compareDay2(int day1, int day2) {
		if (day1 == day2) {
			return 0;
		}
		int yesday = Integer.parseInt(DateUtils.date2String(DateUtils.addDays(
				new Date(), -1), "dd"));
		if (Math.abs(day1 - day2) > 15) {
			if (day1 > day2) {
				if (day1 == yesday) {
					return -1;
				} else {
					return -2;
				}
			} else {
				return 1;
			}
		} else if (day1 < day2) {
			if (day1 == yesday) {
				return -1;
			} else {
				return -2;
			}
		} else {
			return 1;
		}
	}
	
	/**
	 * 获取日期年
	 * @param 日期
	 */
    public static int getYearFrDate(Date dt) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		return cal.get(Calendar.YEAR);
    }
    
    /**
     * 获取日期月
     * @param 日期
     */
    public static int getMonthFrDate(Date dt) {
    	Calendar cal = Calendar.getInstance();
    	cal.setTime(dt);
    	return cal.get(Calendar.MONTH) + 1;
    }
	
	public static void main(String[] args) {
		Date date = new Date();
		int result = DateUtils.compareDay(31, 1);
//		System.out.println(result);
	}
}
