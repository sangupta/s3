package com.sangupta.s3.commands;

import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sangupta.jerry.print.ConsoleTable;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.s3.S3Command;

@Command(name = "ls", description = "List bucket contents")
public class ListBucketContents extends S3Command {
	
	@Option(name = { "--bucket-prefix", "-bp" }, description = "Additional bucket prefix to use along with bucketID")
	private String bucketPrefix;

	@Override
	protected void execute() {
		AmazonS3 s3Client = getS3Client();
		
		ObjectListing objectsInBucket = null;
		try {
			if(AssertUtils.isEmpty(bucketPrefix)) {
				objectsInBucket = s3Client.listObjects(this.baseBucketPath);
			} else {
				objectsInBucket = s3Client.listObjects(this.baseBucketPath, this.bucketPrefix);
			}
		} catch(AmazonServiceException e) {
			AmazonS3Exception s3e = (AmazonS3Exception) e;
			if(s3e.getStatusCode() == 403) {
				System.out.println("Access denied when fetching list of all buckets.");
				return;
			}
		}
		
		List<S3ObjectSummary> summaries = objectsInBucket.getObjectSummaries();
		if(AssertUtils.isEmpty(summaries)) {
			System.out.println("No object in bucket.");
			return;
		}
		
		ConsoleTable table = new ConsoleTable();
		table.addHeaderRow("Name", "Path", "Size", "ETag", "Date");
		for(S3ObjectSummary summary : summaries) {
			table.addRow(summary.getKey(), summary.getKey(), summary.getSize(), summary.getETag(), summary.getLastModified());
		}
		table.write(System.out);
	}
}
