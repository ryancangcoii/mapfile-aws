package com.tooltwist.filemap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.tooltwist.xdata.XDException;
import com.tooltwist.xdata.XSelector;

public class AmazonS3Adaptor implements IFileGroupAdaptor {

	private AmazonS3 s3;
	private String bucketName;

	@Override
	public void init(XSelector config) throws FilemapException {
		
		String accessKey = "";
		String secretKey = "";
		String region = "";
		try {
			bucketName = config.getString("bucketName");
			accessKey = config.getString("accessKey");
			secretKey = config.getString("secretKey");
			region = config.getString("region");
			
			if (region == null || "".equals(region))
				region = Regions.US_WEST_2.getName();
			
		} catch (XDException e) {
			e.printStackTrace();
		}
		
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		s3 = new AmazonS3Client(awsCredentials);
		
		Region awsRegion = null;
		for (Regions regions :Regions.values()) {
			if (regions.getName().equals(region)) {
				awsRegion = Region.getRegion(regions);
				break ;
			}
				
		}
		s3.setRegion(awsRegion);

	}

	@Override
	public OutputStream openForWriting(String relativePath, boolean append) throws IOException {
			
		checkBucketIfExistElseCreate();
		
		File asset = new File(relativePath);
		String assetName = asset.getName();
		
		try {
			PutObjectRequest por = new PutObjectRequest(bucketName, assetName, asset);
			por.setCannedAcl(CannedAccessControlList.PublicRead);
			s3.setEndpoint("s3.amazonaws.com");
			s3.putObject(por);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public InputStream openForReading(String relativePath) throws IOException {
		
		checkBucketIfExistElseCreate();
		
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, relativePath));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        S3ObjectInputStream objectContent = object.getObjectContent();

		return objectContent;
	}

	@Override
	public boolean exists(String relativePath) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDirectory(String relativePath) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFile(String relativePath) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean mkdir(String relativePath) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean mkdirs(String relativePath) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public boolean deleteFile(String relativePath) {
		
		checkBucketIfExistElseCreate();
		
		File asset = new File(relativePath);
		String assetName = asset.getName();
		
		s3.deleteObject(bucketName, assetName);
		
		return true;
	}

	@Override
	public String fileDescription(String relativePath) {
		return "(Amazon S3: " + relativePath + ")"; // ZZZ Should give details of S3 buckeet, etc.
	}
	
	/**
	 * Check bucket if already exist, if not create bucket
	 */
	private void checkBucketIfExistElseCreate() {
		
		boolean isBucketExist = false;
		List<Bucket> listBuckets = s3.listBuckets();
		for (Bucket bucket :listBuckets) {
			if (bucket.getName().equals(bucketName)) {
				isBucketExist = true;
				break;
			}
		}
		
		if (!isBucketExist)
			s3.createBucket(bucketName);
		
	}
	
}
