package com.bigdata.datacenter.datasync.model.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum TipSupTyp {
	// 股票今日提示,股票未来一周提示,股票未来一月提示
	A111_TYP(111,Arrays.asList(new Integer[] {11,12,13,14,15,16,18,20,21,22,31})),
	// 增发配股
	A222_TYP(222,Arrays.asList(new Integer[] {12,13,14})),
	// 限售流通
	A333_TYP(333,Arrays.asList(new Integer[] {16,20})),
	// 风险提示
	A444_TYP(444,Arrays.asList(new Integer[] {22,31})),
	// 基金今日提示,基金未来一周提示,基金未来一月提示
	A555_TYP(555,Arrays.asList(new Integer[] {11,15,18,21,22,23})),
	// 债券今日提示
	A666_TYP(666,Arrays.asList(new Integer[] {15,18,22,24,26,27,28})),
	// 其他大类
	AOTHER_TYP(000,Arrays.asList(new Integer[] {18,19,11,15,21,23,24,27,26,28,22}));
	
	private int supTypCd;
	private List<Integer> typCdLst;
	
	private TipSupTyp(int supTypCd,List<Integer> typCdLst) {
		this.supTypCd = supTypCd;
		this.typCdLst = typCdLst;
	}
	
	public static List<Integer> getSupTypCdLst(Double type_codei) {
		List<Integer> superTypeLst = new ArrayList<Integer>();
        for (TipSupTyp tmp: TipSupTyp.values()) {
        	if (tmp.typCdLst.contains(type_codei.intValue())) {
        		if (!tmp.name().equals(AOTHER_TYP.name())) {
        			superTypeLst.add(tmp.supTypCd);
        		} else {
        			superTypeLst.add(type_codei.intValue());
        		}
        	}
        }
        return superTypeLst;
	}
}
