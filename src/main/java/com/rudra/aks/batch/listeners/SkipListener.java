package com.rudra.aks.batch.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rudra.aks.batch.model.UserBO;

public class SkipListener implements org.springframework.batch.core.SkipListener<UserBO, UserBO> {

	private static Logger logger = LoggerFactory.getLogger(SkipListener.class);
	
	public void onSkipInRead(Throwable t) {
		logger.info("onSkipRead() for exception : " + t.getMessage());
	}

	public void onSkipInWrite(UserBO item, Throwable t) {
		logger.info("onSkipInWrite()" + t.getMessage());
		logger.info("For item : " + item.getUsername() );
	}

	public void onSkipInProcess(UserBO item, Throwable t) {
		logger.info("onSkipProcess() : " + t.getMessage());
		logger.info("For item : " + item);
	}

}
