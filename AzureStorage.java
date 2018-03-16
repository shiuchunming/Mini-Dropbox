import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.io.FileInputStream;

import java.util.Arrays;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class AzureStorage extends Storage {
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=csci4180group2;AccountKey=BZHvGiYbMQmYX1T6RLgcsxAG3RImSQKT/SEpRTPBN5DNrA1fr2PMkYrWHPtTywmBf3L8AxpaGs0s95Fh+ggcNw==;EndpointSuffix=core.windows.net";
    private CloudBlobContainer container;

    public AzureStorage() {
        super();
        try {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(CONNECTION_STRING);
            CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
            container = cloudBlobClient.getContainerReference("helloworldcontainer");
            container.createIfNotExists();
        } catch (Exception e) {

        }
    }

    public boolean upload(String name, String filePath) {
        boolean uploaded = false;
        CloudBlockBlob blob = null;
        try {
            blob = container.getBlockBlobReference(filePath);
        } catch (URISyntaxException e) {

        } catch (StorageException e) {

        }
        if (blob != null) {
            try {
                File source = new File(filePath);
                blob.upload(new FileInputStream(source), source.length());
                uploaded = true;
            } catch (IOException e) {

            } catch (StorageException e) {

            }
        }
        return uploaded;
    }

    public boolean upload(String filePath, byte[] bytes) {
        boolean uploaded = false;
        CloudBlockBlob blob = null;
        try {
            blob = container.getBlockBlobReference(filePath);
        } catch (URISyntaxException e) {

        } catch (StorageException e) {

        }
        if (blob != null) {
            try {
                blob.upload(new ByteArrayInputStream(bytes), bytes.length);
                uploaded = true;
            } catch (IOException e) {

            } catch (StorageException e) {

            }
        }
        return uploaded;
    }

    public boolean upload(String filePath, byte[] bytes, int off, int len) {
        return upload(filePath, Arrays.copyOfRange(bytes, off, off + len));
    }

    public File downloadAsFile(String filePath) {
        File file = null;
        CloudBlockBlob blob = null;
        try {
            blob = container.getBlockBlobReference(filePath);
        } catch (URISyntaxException e) {

        } catch (StorageException e) {

        }
        if (blob != null) {
            try {
                blob.download(new FileOutputStream(filePath));
                file = new File(filePath);
            } catch (Exception e) {

            }
        }
        return file;
    }

    public byte[] download(String filePath) {
        byte[] bytes = null;
        MyFile myFile;
        if ((myFile = downloadFromCache(filePath)) != null) {
            bytes = myFile.bytes;
        } else {
            CloudBlockBlob blob = null;
            try {
                blob = container.getBlockBlobReference(filePath);
            } catch (URISyntaxException e) {

            } catch (StorageException e) {

            }
            if (blob != null) {
                try {
                    blob.downloadAttributes();
                    bytes = new byte[(int)blob.getProperties().getLength()];
                    blob.downloadToByteArray(bytes, 0);
                } catch (StorageException e) {

                }
            }
            cache(filePath, bytes);
        }
        return bytes;
    }

    public boolean delete(String filePath) {
        boolean deleted = false;
        CloudBlockBlob blob = null;
        try {
            blob = container.getBlockBlobReference(filePath);
        } catch (URISyntaxException e) {

        } catch (StorageException e) {

        }
        if (blob != null) {
            try {
                deleted = blob.deleteIfExists();
            } catch (StorageException e) {

            }
        }
        return deleted;
    }
}
