package com.bigdata.datacenter.datasync.utils

import java.sql.Clob
import java.sql.SQLException

/**
 * Created by qq on 2017/6/5.
 */
class StringUtils {

    /**
     * 将clob字段转换为String
     * @param clob
     * @return
     * @throws SQLException
     * @throws IOException
     */
     static String ClobToString(Object clob) throws SQLException, IOException {
         String reString = "";
         if(clob instanceof  Clob){
             Reader is = clob.getCharacterStream();// 得到流
             BufferedReader br = new BufferedReader(is);
             String s = br.readLine();
             StringBuffer sb = new StringBuffer();
             while (s != null) {// 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
                 sb.append(s+"\n");
                 s = br.readLine();
             }
             reString = sb.toString();
         }else{
             reString = clob;
         }
        return reString;
    }
}
