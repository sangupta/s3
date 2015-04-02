package com.sangupta.s3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.jerry.util.FileUtils;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class S3Command implements Runnable {
	
	public static final String AWS_S3_ACCESS_KEY = "aws.s3.access.key";
	public static final String AWS_S3_SECRET_KEY = "aws.s3.secret.key";
	public static final String AWS_S3_BUCKET = "aws.s3.bucket";

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

	/**
	 * Function that reads AWS details from a properties file if they
	 * are not supplied on the command line.
	 * 
	 * @return
	 */
	private boolean readAWSDetails() {
		if(AssertUtils.areNotEmpty(this.awsAccessKey, this.awsSecretKey, this.baseBucketPath)) {
			// all good
			return true;
		}
		
		// atleast one is missing
		// find the right file to use
		Properties properties = discoverMergedProperties(".s3.config");
		if(properties == null || properties.isEmpty()) {
			System.out.println("ERROR: must define the AWS access key, secret key, and bucket to use.");
			return false;
		}
		
		if(AssertUtils.isEmpty(this.awsAccessKey)) {
			this.awsAccessKey = properties.getProperty(AWS_S3_ACCESS_KEY);
			if(AssertUtils.isEmpty(this.awsAccessKey)) {
				System.out.println("ERROR: AWS access key is needed.");
				return false;
			}
		}
		
		if(AssertUtils.isEmpty(this.awsSecretKey)) {
			this.awsSecretKey = properties.getProperty(AWS_S3_SECRET_KEY);
			if(AssertUtils.isEmpty(this.awsSecretKey)) {
				System.out.println("ERROR: AWS secret key is needed.");
				return false;
			}
		}
		
		if(AssertUtils.isEmpty(this.baseBucketPath)) {
			this.baseBucketPath = properties.getProperty(AWS_S3_BUCKET);
			if(AssertUtils.isEmpty(this.baseBucketPath)) {
				System.out.println("ERROR: AWS S3 bucket is needed.");
				return false;
			}
		}
		
		return true;
	}

	private Properties discoverMergedProperties(String fileName) {
		Properties global = readPropertyFile(new File(FileUtils.getUsersHomeDirectory(), fileName));
		Properties local = readPropertyFile(new File(fileName));

		if(local.isEmpty()) {
			return global;
		}
		
		if(global.isEmpty()) {
			return local;
		}
		
		// merge - preference to local
		Set<Object> keys = local.keySet();
		for(Object key : keys) {
			Object value = local.get(key);
			
			// overwrite in global
			global.put(key, value);
		}
		
		return global;
	}
	
	private Properties readPropertyFile(File file) {
		Properties properties = new Properties();
		
		// read from user home
		if(file.exists() && file.isFile() && file.canRead()) {
			InputStream is = null;
			try {
				is = org.apache.commons.io.FileUtils.openInputStream(file);
				properties.load(is);
			} catch (IOException e) {
				// eat up
				properties.clear();
			} finally {
				IOUtils.closeQuietly(is);
			}
		}
		
		return properties;
	}
	
}
