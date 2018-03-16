import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyDedup {
    public static void upload(int minimumChunkSize, int averageChunkSize, int maximumChunkSize, int multiplier, String filePath, String storageName, Index index) {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            // e.printStackTrace();
        }
        if (fileInputStream != null) {
            if (index.contains(filePath)) {
                System.out.println("WARNING: File \"" + filePath + "\" already exists in " + storageName + " storage");
                delete(filePath, storageName, index);
            }
            long numberOfChunks = 0;
            long numberOfUniqueChunks = 0;
            long numberOfBytesWithDeduplication = 0;
            long numberOfBytesWithoutDeduplication = 0;
            int len;
            byte[] bytes = new byte[Storage.SLICE_SIZE];
            BufferedInputStream BufferedInputStream = new BufferedInputStream(fileInputStream);
            try {
                while ((len = BufferedInputStream.read(bytes)) > 0) {
                    numberOfBytesWithDeduplication += uploadSlice(minimumChunkSize, averageChunkSize, maximumChunkSize, multiplier, filePath, index, bytes, len);
                    numberOfBytesWithoutDeduplication += len;
                }
            } catch (IOException e) {
                // e.printStackTrace();
            }
            Map<String, Integer> occurrences = index.getOccurrences();
            for (String key: occurrences.keySet()) {
                numberOfChunks += occurrences.get(key);
                numberOfUniqueChunks += 1;
            }
            System.out.println("Report Output:");
            System.out.println("Total number of chunks in storage: " + numberOfChunks);
            System.out.println("Number of unique chunks in storage: " + numberOfUniqueChunks);
            System.out.println("Number of bytes in storage with deduplication: " + numberOfBytesWithDeduplication);
            System.out.println("Number of bytes in storage without deduplication: " + numberOfBytesWithoutDeduplication);
            System.out.println("Space saving: " + (numberOfBytesWithoutDeduplication > 0 ? (1 - numberOfBytesWithDeduplication * 1.0 / numberOfBytesWithoutDeduplication) : 0));
            index.synchronize();
        } else {
            System.out.println("ERROR: File \"" + filePath + "\" does not exist");
        }
    }

    public static int uploadSlice(int minimumChunkSize, int averageChunkSize, int maximumChunkSize, int multiplier, String filePath, Index index, byte[] bytes, int length) {
        int numberOfBytesWithDeduplication = 0;
        List<String> checksums = new ArrayList<String>();
        if (length > 0) {
            Fingerprint fingerprint = new Fingerprint(bytes, minimumChunkSize, averageChunkSize, maximumChunkSize, multiplier, length);
            int len = 0;
            int rfp = -1;
            for (int i = 0; i < fingerprint.sizeOfPattern; i++) {
                len = fingerprint.getAnchorLen();
                rfp = fingerprint.previous;
                String checksum = fingerprint.checksum(i, len);
                if (index.add(Index.convertToASCII(checksum), bytes, i, len)) {
                    numberOfBytesWithDeduplication += len;
                }
                checksums.add(checksum);
                i = fingerprint.curInd - 1;
            }
        }
        index.addAssociation(filePath, checksums);
        return numberOfBytesWithDeduplication;
    }

    public static void download(String filePath, String storageName, Storage storage, Index index) {
        List<String> checksums = index.getChecksums(filePath);
        if (checksums != null) {
            try {
                if (filePath.lastIndexOf("/") > -1) {
                    String path = filePath.substring(0, filePath.lastIndexOf(File.separator));
                    if (Storage.createDirectories(path)) {
                        System.out.println("WARNING: Path \"" + path + "\" reconstructed");
                    }
                }
                FileOutputStream fileOutputStream = new FileOutputStream(filePath + ".download");
                for (String checksum: checksums) {
                    fileOutputStream.write(storage.download(Index.convertToASCII(checksum)));
                }
                fileOutputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("ERROR: File \"" + filePath + "\" does not exist in " + storageName + " storage");
        }
    }

    public static void delete(String filePath, String storageName, Index index) {
        List<String> checksums = index.getChecksums(filePath);
        if (checksums != null) {
            for (String checksum: checksums) {
                index.subtractOneOccurrence(Index.convertToASCII(checksum));
            }
            index.removeAssociation(filePath);
        } else {
            System.out.println("ERROR: File \"" + filePath + "\" does not exist in " + storageName + " storage");
        }
        index.synchronize();
    }

    public static void main(String[] args) {
        System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("https.proxyPort", "8000");
        System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("http.proxyPort", "8000");
        String operation = args[0];
        if (operation.equals("upload")) {
            int minimumChunkSize = Integer.parseInt(args[1]);
            int averageChunkSize = Integer.parseInt(args[2]);
            int maximumChunkSize = Integer.parseInt(args[3]);
            int multiplier = Integer.parseInt(args[4]);
            String filePath = args[5];
            String storageName = args[6];
            Index index = new Index(Storage.get(storageName));
            upload(minimumChunkSize, averageChunkSize, maximumChunkSize, multiplier, filePath, storageName, index);
        } else if (operation.equals("download")) {
            String filePath = args[1];
            String storageName = args[2];
            Storage storage = Storage.get(storageName);
            Index index = new Index(storage);
            download(filePath, storageName, storage, index);
        } else if (operation.equals("delete")) {
            String filePath = args[1];
            String storageName = args[2];
            Index index = new Index(Storage.get(storageName));
            delete(filePath, storageName, index);
        }
    }
}
