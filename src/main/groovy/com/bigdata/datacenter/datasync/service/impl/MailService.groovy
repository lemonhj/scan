package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import org.apache.log4j.Logger
import org.springframework.mail.SimpleMailMessage
import org.springframework.mail.javamail.JavaMailSenderImpl

@Service(value = "mail")
class MailService {
	static Logger logger = Logger.getLogger(MailService.class);
	@Autowired
	JavaMailSenderImpl mailSender;
	@Autowired
	SimpleMailMessage mailMge;
	static String dataIp = ""

	static{
		dataIp = PropertiesUtil.getProperty("scan.identification");
		if(dataIp=="" || dataIp=="null"){
			dataIp = "test";
		}
	}

	void sendErrLogMail(String sub, String errLog) {
		try {
			if (PropertiesUtil.getProperty("mail.send").equals("true")) {
				mailMge.setSubject(PropertiesUtil.getProperty("mongodb.server") + " " + sub + " " + "服务错误日志");// 主题
				mailMge.setText(errLog);// 邮件内容
				mailSender.send(mailMge);
			}
		} catch (Exception e) {
			logger.error(e.toString());
		}
	}

	/**
	 * 邮件发送
	 * @param sub
	 * @param text
	 */
	void sendMail(String sub, String text) {
		try {
			if (PropertiesUtil.getProperty("mail.send").equals("true")) {
				mailMge.setSubject("服务器["+dataIp+"]扫描:"+sub);// 主题
				mailMge.setText(text);// 邮件内容
				mailSender.send(mailMge);
			}
		} catch (Exception e) {
			logger.error("邮件发送失败，错误信息为："+e.toString());
		}
	}
}
