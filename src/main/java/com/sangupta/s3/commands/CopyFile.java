package com.sangupta.s3.commands;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.s3.S3Command;

@Command(name = "cp", description = "Copy file from one S3 bucket to another")
public class CopyFile extends S3Command {
	
	@Arguments
	private List<String> filesToCopy;

	@Override
	protected void execute() {
		if(AssertUtils.isEmpty(this.filesToCopy)) {
			System.out.println("No key specified to be copied.");
			return;
		}

		if(this.filesToCopy.size() != 2) {
			System.out.println("Must specify exactly two keys to copy: one existing, second the newer one");
			return;
		}
		
		// create client
		AmazonS3 s3Client = getS3Client();
		
		CopyObjectResult result = null;
		try {
			result = s3Client.copyObject(this.baseBucketPath, this.filesToCopy.get(0), this.baseBucketPath, this.filesToCopy.get(1));
			if(result != null) {
				System.out.println("File copied from key " + this.filesToCopy.get(0) + " to " + this.filesToCopy.get(1));
			} else {
				System.out.println("Unable to copy file: " + this.filesToCopy.get(0));
			}
		} catch(AmazonServiceException e) {
			AmazonS3Exception s3e = (AmazonS3Exception) e;
			if(s3e.getStatusCode() == 403) {
				System.out.println("Access denied when copying keys.");
				return;
			}
		}
	}

}
