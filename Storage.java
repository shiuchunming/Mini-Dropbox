import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Storage {
    public static final int SLICE_SIZE = 80 * 1024 * 1024; // 80MB

    public static Storage get(String storageName) {
        Storage storage;
        if (storageName.equals("local")) {
            storage = new LocalStorage();
        } else if (storageName.equals("s3")) {
            storage = new AmazonS3Storage();
        } else {
            storage = new AzureStorage();
        }
        return storage;
    }

    public static boolean createDirectories(String path) {
        boolean created = false;
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
            created = true;
        }
        return created;
    }

    public static boolean remove(String path) {
        return new File(path).delete();
    }

    public static byte[] read(String filePath) {
        byte[] bytes = null;
        File file = new File(filePath);
        if (file.exists()) {
            FileInputStream fileInputStream = null;
            try {
                bytes = new byte[(int) file.length()];
                fileInputStream = new FileInputStream(file);
                fileInputStream.read(bytes);
            } catch (IOException e) {
                // e.printStackTrace();
            }
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    // e.printStackTrace();
                }
            }
        }
        return bytes;
    }

    public static byte[] convertToBytes(Map map) {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(map);
            bytes = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {

        }
        return bytes;
    }

    public static Object convertToObject(byte[] bytes) {
        Object object = null;
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            object = objectInputStream.readObject();
        } catch (IOException e) {

        } catch (ClassNotFoundException e) {

        }
        return object;
    }

    private List<MyFile> myFiles;

    public Storage() {
        myFiles = new ArrayList<MyFile>();
    }

    private void enqueue(MyFile myFile) {
        myFiles.add(myFiles.size(), myFile);
    }

    private MyFile dequeue() {
        MyFile myFile = null;
        if (myFiles.size() > 0) {
            myFile = myFiles.get(0);
            myFiles.remove(0);
        }
        return myFile;
    }

    protected MyFile downloadFromCache(String filePath) {
        MyFile myFile = null;
        for (MyFile m : myFiles) {
            if (m.name.equals(filePath)) {
                myFile = m;
                break;
            }
        }
        return myFile;
    }

    protected void cache(String filePath, byte[] bytes) {
        if (myFiles.size() == 5) {
            dequeue();
        }
        enqueue(new MyFile(filePath, bytes));
    }

    public abstract boolean upload(String name, String filePath);
    public abstract boolean upload(String filePath, byte[] bytes);
    public abstract boolean upload(String filePath, byte[] bytes, int offset, int len);
    public abstract File downloadAsFile(String filePath);
    public abstract byte[] download(String filePath);
    public abstract boolean delete(String filePath);

    public static void main(String[] args) {
        // System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        // System.setProperty("https.proxyPort", "8000");
        // System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        // System.setProperty("http.proxyPort", "8000");
        Storage storage = new LocalStorage();
        // storage.upload("compile.txt", "compile.txt");
        storage.downloadAsFile("compile.txt");
    }
}
