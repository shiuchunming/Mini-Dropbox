import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalStorage extends Storage {
    private static final String BASE_PATH = "./data";

    public LocalStorage() {
        File file = new File(BASE_PATH);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    public boolean upload(String name, String filePath) {
        try {
        System.out.println(name + " " + filePath);
        new File(filePath).renameTo(new File(BASE_PATH + File.separator + name));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public boolean upload(String filePath, byte[] bytes) {
        return upload(filePath, bytes, 0, bytes.length);
    }

    public boolean upload(String filePath, byte[] bytes, int off, int len) {
        boolean uploaded = false;
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(BASE_PATH + "/" + filePath);
        } catch (FileNotFoundException e) {

        }
        if (fileOutputStream != null) {
            try {
                fileOutputStream.write(bytes, off, len);
            } catch (IOException e) {

            }
            try {
                fileOutputStream.close();
                uploaded = true;
            } catch (IOException e) {

            }
        }
        return uploaded;
    }

    public File downloadAsFile(String filePath) {
      File file = null;
      try {
          file = new File(BASE_PATH + File.separator + filePath);
      } catch (Exception e) {

      }
      return file;
    }

    public byte[] download(String filePath) {
        return Storage.read(BASE_PATH + "/" + filePath);
    }

    public boolean delete(String filePath) {
        return Storage.remove(BASE_PATH + "/" + filePath);
    }
}
