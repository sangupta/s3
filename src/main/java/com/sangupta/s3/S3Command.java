package com.sangupta.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class S3Command implements Runnable {

	@Option(type = OptionType.GLOBAL, name = { "--access-key", "-ak" }, description = "AWS Access Key to use")
	protected String awsAccessKey;
	
	@Option(type = OptionType.GLOBAL, name = { "--secret-key", "-sk" }, description = "AWS Secret Key to use")
	protected String awsSecretKey;
	
	@Option(type = OptionType.GLOBAL, name = { "--bucket", "-b" }, description = "The base bucket path to use")
	protected String baseBucketPath;
	
	@Override
	public void run() {
		// first read the access key/secret key/bucket details
		// environment, home dir, local dir or command line
		final boolean allWell = readAWSDetails();
		
		if(!allWell) {
			return;
		}
		
		// if everything is fine
		execute();
		
		// add a new line at the end
		System.out.println();
	}

	protected abstract void execute();
	
	protected AmazonS3 getS3Client() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.awsAccessKey, this.awsSecretKey);
		return new AmazonS3Client(awsCreds);
	}

	private boolean readAWSDetails() {
		// TODO Auto-generated method stub
		return true;
	}
	
}
