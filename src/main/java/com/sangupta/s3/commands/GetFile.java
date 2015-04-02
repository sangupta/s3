package com.sangupta.s3.commands;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.s3.S3Command;

@Command(name = "get", description = "Upload a file to the S3 bucket")
public class GetFile extends S3Command {
	
	@Arguments(description = "A space separated list of files/keys that need to be downlaoded")
	private List<String> filesToDownload;

	@Override
	protected void execute() {
		if(AssertUtils.isEmpty(this.filesToDownload)) {
			System.out.println("Specify the file(s) to be downloaded.");
			return;
		}
		
		// create client
		AmazonS3 s3Client = getS3Client();
		
		S3Object result = null;
		try {
			for(String key : this.filesToDownload) {
				result = s3Client.getObject(this.baseBucketPath, key); 
				boolean downloaded = false;
				
				if(result != null) {
					String name = result.getKey();
					S3ObjectInputStream is = result.getObjectContent();
					File file = new File(name);
					try {
						Files.copy(is, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
						System.out.println("File " + name + " saved successfully with ETag: " + result.getObjectMetadata().getETag());
						downloaded = true;
					} catch (IOException e) {
						// eat up
					}
				}
				
				if(!downloaded) {
					System.out.println("Unable to download file: " + key);
				}
			}
		} catch(AmazonServiceException e) {
			AmazonS3Exception s3e = (AmazonS3Exception) e;
			if(s3e.getStatusCode() == 403) {
				System.out.println("Access denied when fetching list of all buckets.");
				return;
			}
		}
		
		System.out.println("Done downloading all files.");
	}

}
