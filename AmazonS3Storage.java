import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;

public class AmazonS3Storage extends Storage {
    private AmazonS3 amazonS3;
    private String bucketName;

    public AmazonS3Storage() {
        super();
        amazonS3 = AmazonS3ClientBuilder.defaultClient();
        bucketName = "csci4180group2bucket";
        try {
            if (!amazonS3.doesBucketExistV2(bucketName)) {
                amazonS3.createBucket(bucketName);
            }
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
        }
    }

    public boolean upload(String name, String filePath) {
        boolean uploaded = false;
        try {
            amazonS3.putObject(bucketName, name, new File(filePath));
            uploaded = true;
        } catch (Exception e) {
            
        }
        return uploaded;
    }

    public boolean upload(String filePath, byte[] bytes) {
        return upload(filePath, bytes, 0, bytes.length);
    }

    public boolean upload(String filePath, byte[] bytes, int off, int len) {
      boolean uploaded = false;
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentLength(len);
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes, off, len);
      try {
          amazonS3.putObject(bucketName, filePath, byteArrayInputStream, objectMetadata);
          uploaded = true;
      } catch (AmazonServiceException e) {
          e.printStackTrace();
      }
      return uploaded;
    }

    public File downloadAsFile(String filePath) {
        File file = new File(filePath);
        try {
            S3Object o = amazonS3.getObject(bucketName, filePath);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(file);
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();            
        } catch (Exception e) {
            file = null;
        }
        return file;
    }

    public byte[] download(String filePath) {
        byte[] bytes = null;
        MyFile myFile;
        if ((myFile = downloadFromCache(filePath)) != null) {
            bytes = myFile.bytes;
        } else {
            S3Object s3Object = null;
            try {
                s3Object = amazonS3.getObject(bucketName, filePath);
            } catch (AmazonS3Exception e) {

            }
            if (s3Object != null) {
                InputStream inputStream = s3Object.getObjectContent();
                try {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    int len;
                    while((len = inputStream.read()) != -1) {
                        byteArrayOutputStream.write(len);
                    }
                    bytes = byteArrayOutputStream.toByteArray();
                } catch (IOException e) {

                }
                cache(filePath, bytes);
            }
        }
        return bytes;
    }

    public boolean delete(String filePath) {
        boolean deleted = false;
        try {
            amazonS3.deleteObject(bucketName, filePath);
            deleted = true;
        } catch (AmazonServiceException e) {

        }
        return deleted;
    }
}
