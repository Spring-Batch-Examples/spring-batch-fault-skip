package com.rudra.aks.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

import com.rudra.aks.batch.listeners.SkipListener;

public class SkipPolicyImpl implements SkipPolicy {
	
	private static Logger logger = LoggerFactory.getLogger(SkipListener.class);
	

	private int count;
	private Throwable setterException;
	
	/**
	 * No Argument Constructor
	 */
	public SkipPolicyImpl() {
		super();
	}

	/**
	 * Constructor with skipcount & exception to skip while processing
	 * @param skipCount
	 * @param ex
	 */
	public SkipPolicyImpl(int setcount, Throwable ex) {
		this.count = setcount;
		this.setterException = ex;
	}

	public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
		
		if(setterException instanceof Throwable && skipCount <= count) {
			logger.error("Exception : " + t.getMessage());
			FlatFileParseException ffpe = ((FlatFileParseException)t);
			logger.error(" At line " + ffpe.getLineNumber() + " for : " + ffpe.getInput()); 
			return true;
		}
		return false;
	}

	
	
}
