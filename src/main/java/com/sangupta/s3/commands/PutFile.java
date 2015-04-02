package com.sangupta.s3.commands;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.s3.S3Command;

@Command(name = "put", description = "Upload a file to the S3 bucket")
public class PutFile extends S3Command {
	
	@Arguments(description = "A space delimited list of filename that need to be uploaded to S3")
	private List<String> filesToUpload;

	@Override
	protected void execute() {
		if(AssertUtils.isEmpty(this.filesToUpload)) {
			System.out.println("Specify the file(s) to be uploaded.");
			return;
		}
		
		// check and get all files
		List<File> files = new ArrayList<>();
		for(String filePath : filesToUpload) {
			File file = new File(filePath);
			if(!(file.exists() && file.isFile() && file.canRead())) {
				System.out.println("File " + filePath + " will be ignored - either it does not exist, or is not a file, or can't be read.");
				continue;
			}
			
			files.add(file);
		}
		
		// check if we have something to upload
		if(AssertUtils.isEmpty(files)) {
			System.out.println("No file to upload.");
			return;
		}
		
		// create client
		AmazonS3 s3Client = getS3Client();
		
		PutObjectResult result = null;
		try {
			for(File file : files) {
				result = s3Client.putObject(this.baseBucketPath, file.getName(), file);
				if(result != null) {
					System.out.println("File " + file.getName() + " uploaded successfully with ETag: " + result.getETag());
				} else {
					System.out.println("Unable to upload file: " + file.getName());
				}
			}
		} catch(AmazonServiceException e) {
			AmazonS3Exception s3e = (AmazonS3Exception) e;
			if(s3e.getStatusCode() == 403) {
				System.out.println("Access denied when fetching list of all buckets.");
				return;
			}
		}
		
		System.out.println("Done uplaoding all files.");
	}

}
