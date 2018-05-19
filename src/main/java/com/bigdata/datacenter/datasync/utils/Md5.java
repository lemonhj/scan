package com.bigdata.datacenter.datasync.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/***
 * 安全工具
 *
 */
public class Md5 {
	/**
	 * md5加密
	 * 
	 * @param plainText
	 * @return
	 */
	public static String md5(String plainText) {
		byte[] secretBytes = null;
		try {
			secretBytes = MessageDigest.getInstance("md5").digest(plainText.getBytes());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("no md5!");
		}
		String md5code = new BigInteger(1, secretBytes).toString(16);// 16进制数字
		// 如果生成数字未满32位，需要前面补0
		for (int i = 0; i < 32 - md5code.length(); i++) {
			md5code = "0" + md5code;
		}
		return md5code;
	}

	//md5(lower(md5(upper(username))) + password)
	public static void main(String argv[]){
//		String username = "lzw";
//		String pwd = "123";
//		String enp = Md5.md5((Md5.md5(username.toUpperCase())).toLowerCase() + pwd);
//		System.out.println("enp:"+ enp);
		// fe5e519872037ed4ef568247d14b0ec5  fe5e519872037ed4ef568247d14b0ec5
	}
}
