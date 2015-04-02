package com.sangupta.s3.commands;

import io.airlift.airline.Command;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.sangupta.jerry.print.ConsoleTable;
import com.sangupta.jerry.util.AssertUtils;
import com.sangupta.s3.S3Command;

/**
 * List all items in the bucket
 * 
 * @author sangupta
 *
 */
@Command(name = "lsb", description = "List all buckets")
public class ListBuckets extends S3Command {

	@Override
	protected void execute() {
		AmazonS3 s3Client = getS3Client();
		
		List<Bucket> buckets = null;
		try {
			buckets = s3Client.listBuckets();
		} catch(AmazonServiceException e) {
			AmazonS3Exception s3e = (AmazonS3Exception) e;
			if(s3e.getStatusCode() == 403) {
				System.out.println("Access denied when fetching list of all buckets.");
				return;
			}
		}
		
		if(AssertUtils.isEmpty(buckets)) {
			System.out.println("No buckets found.");
			return;
		}
		
		ConsoleTable table = new ConsoleTable();
		table.addHeaderRow("Name", "Owner", "Creation Date");
		for(Bucket bucket : buckets) {
			table.addRow(bucket.getName(), bucket.getOwner(), bucket.getCreationDate());
		}
		
		System.out.println(table);
	}

}
