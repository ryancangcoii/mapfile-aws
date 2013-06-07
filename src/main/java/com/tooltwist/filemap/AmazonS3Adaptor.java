package com.tooltwist.filemap;

<<<<<<< HEAD
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
=======
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
<<<<<<< HEAD
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
=======
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.tooltwist.xdata.XDException;
import com.tooltwist.xdata.XSelector;

public class AmazonS3Adaptor implements IFileGroupAdaptor {
	
	private String baseDir = "/tmp";
	private AmazonS3Client s3;
	private String bucketName;

	public void init(XSelector config) throws FilemapException {
		
		//check the configuration specified
		String accessKey = "";
		String secretKey = "";
		String region = "";
		
		try {
			bucketName = config.getString("bucketName");
			accessKey = config.getString("accessKey");
			secretKey = config.getString("secretKey");
			region = config.getString("region");
			
		} catch (XDException e) {
			throw new FilemapException("Error parsing config.");
		}
<<<<<<< HEAD
		
		try {
			//set s3 credentials 
			AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
			s3 = new AmazonS3Client(awsCredentials);
		} catch (Exception e) {
			throw new FilemapException("Error initializing s3 object");
		}
		
=======
		
		//set s3 credentials 
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		s3 = new AmazonS3Client(awsCredentials);
		
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
		//initialize region
		try {
			
			if (region == null || "".equals(region))
				region = Regions.US_WEST_2.getName();
			
			Region awsRegion = null;
			for (Regions regions :Regions.values()) {
				if (regions.getName().equals(region)) {
					awsRegion = Region.getRegion(regions);
					break ;
				}
					
			}
			s3.setRegion(awsRegion);
			
		} catch (Exception e) {
			throw new FilemapException("Error setting region.");
		}

	}
	
<<<<<<< HEAD
=======
	@Override
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	public boolean exists(String relativePath) throws FileNotFoundException {
		
		boolean exists = false;
		
		ObjectListing listObjects = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(relativePath));
        List<S3ObjectSummary> commonPrefixes = listObjects.getObjectSummaries();
        if (commonPrefixes.size() > 0)
        	exists = true;
		
		return exists;
	}

<<<<<<< HEAD
	public boolean isDirectory(String relativePath) throws FileNotFoundException {
		
		boolean isDirectory = true;
	
		return isDirectory;
	}

	public boolean isFile(String relativePath) throws FileNotFoundException {
		
		boolean isFile = false;
		
        ObjectListing listObjects = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(relativePath));
        List<S3ObjectSummary> commonPrefixes = listObjects.getObjectSummaries();
        for (S3ObjectSummary s3Obect : commonPrefixes) {
        	String key = s3Obect.getKey();
        	
        	if (relativePath.equals(key)) {
        		isFile = true;
        		break;
        	}
        	
        }

        return isFile;
	}

=======
	@Override
	public boolean isDirectory(String relativePath) throws FileNotFoundException {
		
		String path = fullPath(relativePath);
		File file = new File(path);
		boolean exists = file.isDirectory();
		return exists;
	}

	@Override
	public boolean isFile(String relativePath) throws FileNotFoundException {
		
		String path = fullPath(relativePath);
		File file = new File(path);
		boolean exists = file.isFile();
		return exists;
	}

	@Override
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	public boolean mkdir(String relativePath) throws FileNotFoundException {
		
		return true;
		 
	}

<<<<<<< HEAD
=======
	@Override
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	public boolean mkdirs(String relativePath) throws FileNotFoundException {
		
		return mkdir(relativePath);
		
	}

<<<<<<< HEAD
=======
	@Override
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	public AwsPluginOutputStream openForWriting(String relativePath, boolean append) throws IOException {
			
		createBucketIfNotExist(bucketName);
		
		
		AwsPluginOutputStream outputStream = new AwsPluginOutputStream();
		outputStream.setRelativePath(relativePath);
		outputStream.setS3(s3);
		outputStream.setBucketName(bucketName);
		
		return outputStream;
		
	}

<<<<<<< HEAD
=======
	@Override
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	public InputStream openForReading(String relativePath) throws IOException {
		
		createBucketIfNotExist(bucketName);
        
        while (relativePath.startsWith("/"))
        	relativePath = relativePath.substring(1);
        relativePath = relativePath.replace("//", "/");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, relativePath));
        
        return object.getObjectContent();

	}
	
<<<<<<< HEAD
	public Iterable<String> files(String directoryRelativePath) {
		
=======
	@Override
	public Iterable<String> files(String directoryRelativePath) { 
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
        LinkedList<String> result = new LinkedList<String>();
        
        ObjectListing listObjects = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(directoryRelativePath));
        List<S3ObjectSummary> commonPrefixes = listObjects.getObjectSummaries();
        for (S3ObjectSummary s3Obect : commonPrefixes) {
        	String key = s3Obect.getKey();
        	
        	if (directoryRelativePath.endsWith("/"))
        	key = key.substring(directoryRelativePath.length());
        	
        	//remove leading slashes
        	if (key.startsWith("/"))
        		key = key.substring(1);	
        	
        	//include only if obect is not subfolder.
        	if (!key.equals("") && !key.endsWith("/"))
        		result.add(key);
        	
        }

        return result;
    }
	
	
	 /** Delete an object - if they key has any leading slashes, they are removed.
     */
<<<<<<< HEAD
	public boolean delete(String relativePath) {
=======
	public boolean deleteFile(String relativePath) {
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
		
		if (relativePath.startsWith("/"))
			relativePath = relativePath.substring(1);
		
		s3.deleteObject(bucketName, relativePath);
		
		return true;
	}

	public String fileDescription(String relativePath) {
		return s3.getResourceUrl(bucketName, relativePath);
<<<<<<< HEAD
	}
	
	/**
	 * Check bucket if already exist, if not create bucket
	 */
	private void createBucketIfNotExist(String bucketName) {
		
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
	

=======
	}
	
	private String fullPath(String relativePath) {
		if (relativePath.startsWith("/"))
			return baseDir + relativePath;
		return baseDir + "/" + relativePath;
	}
	
	/**
	 * Check bucket if already exist, if not create bucket
	 */
	private void createBucketIfNotExist(String bucketName) {
		
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
>>>>>>> 8924560c88a9925c86754d52fb8917166c109902
	
}
