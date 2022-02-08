package com.netbric.s5.conductor;

import javax.servlet.http.HttpServletRequest;

import com.netbric.s5.conductor.exception.InvalidParamException;
import org.apache.commons.lang3.StringUtils;

public class Utils
{
	public static String getParamAsString(HttpServletRequest request, String name) throws InvalidParamException
	{
		String v = request.getParameter(name);
		if (StringUtils.isEmpty(v))
		{
			throw new InvalidParamException("Invalid argument:" + name);
		}
		return v;
	}

	public static String getParamAsString(HttpServletRequest request, String name, String defVal)
	{
		String v = request.getParameter(name);
		if (StringUtils.isEmpty(v))
		{
			return defVal;
		}
		return v;
	}

	public static String getParamAsStringPwd(HttpServletRequest request, String string, String defVal)
	{
		String v = request.getParameter(string);
		if (StringUtils.isEmpty(v))
		{
			return defVal;
		}
		return v;
	}

	public static int getParamAsInt(HttpServletRequest request, String string, int defVal)
	{
		String v = request.getParameter(string);
		if (StringUtils.isEmpty(v))
		{
			return defVal;
		}
		return Integer.parseInt(v);
	}

	public static int getParamAsInt(HttpServletRequest request, String string) throws InvalidParamException
	{
		String v = request.getParameter(string);
		if (StringUtils.isEmpty(v))
		{
			throw new InvalidParamException("Invalid argument:" + string);
		}
		return Integer.parseInt(v);
	}

	public static long getParamAsLong(HttpServletRequest request, String string, long defVal)
	{
		String v = request.getParameter(string);
		if (StringUtils.isEmpty(v))
		{
			return defVal;
		}
		// return Integer.parseInt(v);
		return Long.parseLong(v);
	}

	public static long getParamAsLong(HttpServletRequest request, String string) throws InvalidParamException
	{
		String v = request.getParameter(string);
		if (StringUtils.isEmpty(v))
		{
			throw new InvalidParamException("Invalid argument:" + string);
		}
		return Long.parseLong(v);
	}


	public static Object delayFormat(String format, Object... args) {
		return new Object() {
			@Override
			public String toString() {
				return String.format(format, args);
			}
		};

	}
}
