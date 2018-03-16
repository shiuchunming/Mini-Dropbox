import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.FileNotFoundException;

public class Index {
    private final String FILE_PATH = "mydedup.index";
    private Storage storage;
    private Map<String, Integer> occurrences;
    private Map<String, List<String>> association;



    @SuppressWarnings("unchecked")
    public Index(Storage storage) {

      association = new HashMap<String, List<String>>();
      occurrences = new HashMap<String, Integer>();



        this.storage = storage;



        try {
          File file = storage.downloadAsFile(FILE_PATH);
          if (file != null) {
              FileReader fileReader = null;
              try {
                fileReader = new FileReader(file);
              } catch (FileNotFoundException e) {

              }
              if (fileReader != null) {
                  try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                      String line;
                      boolean finishReadRecipt = false;
                      while ((line = br.readLine()) != null) {
                          String parts[] = line.split(" ");
                          if(parts.length == 2){
                          int numberOfChunks = Integer.parseInt(parts[1]);
                          if(!finishReadRecipt){
                          //  System.out.println("File Name: " + parts[0]);
                          //  System.out.println("File Size: " + numberOfChunks);
                          List<String> checksums = new ArrayList<String>()
                          ;
                          for(int i=0; i<numberOfChunks; i++){
                          line = br.readLine();
                          // System.out.println("Chunk name: " + line);
                          checksums.add(line);
                          }
                          association.put(parts[0],checksums);
                          }else{
                          //  System.out.println("Index Name: " + parts[0]);
                          //  System.out.println("Number of referenced index: " + numberOfChunks);
                          occurrences.put(parts[0],numberOfChunks);
                          }
                          }else if(parts.length==1){
                          if(parts[0].charAt(0) == '/'){
                          finishReadRecipt = true;
                          }
                          }else{
                          //  System.out.println("Abnormal case parts: " + parts.length);
                          }
                      }
                  }catch (Exception e){
                    e.printStackTrace();
                  }
              }
          } else {
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
    }

    public boolean add(String checksum, byte[] bytes, int offset, int len) {
        boolean uploaded;
        if (occurrences.get(checksum) == null) {
            occurrences.put(checksum, 1);
            uploaded = storage.upload(checksum, bytes, offset, len);
        } else {
            occurrences.put(checksum, occurrences.get(checksum) + 1);
            uploaded = false;
        }
        return uploaded;
    }

    public void addAssociation(String filePath, List<String> checksums) {
        List<String> list = association.get(filePath);
        if (list == null) {
            list = checksums;
        } else {
            for (String checksum: checksums) {
                list.add(checksum);
            }
        }
        association.put(filePath, list);
    }

    public void subtractOneOccurrence(String checksum) {
        int count = occurrences.get(checksum) - 1;
        if (count == 0) {
            occurrences.remove(checksum);
            storage.delete(checksum);
        } else {
            occurrences.put(checksum, count);
        }
    }

    public void removeAssociation(String filePath) {
        association.remove(filePath);
    }

    public boolean contains(String filePath) {
        boolean flag = false;
        if (association.get(filePath) != null) {
            flag = true;
        }
        return flag;
    }

    public List<String> getChecksums(String filePath) {
        return association.get(filePath);
    }

    public Map<String, Integer> getOccurrences() {
        return occurrences;
    }

    public Set<String> getStoredFilePaths() {
        return association.keySet();
    }

    @SuppressWarnings("unchecked")
    public void synchronize() {
        Map map = new HashMap();
        map.put("association", association);
        map.put("occurrences", occurrences);


        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter("./" + FILE_PATH));

            // System.out.println("-------------  ./" + FILE_PATH);
            for (String key: association.keySet()) {


              // System.out.prin
                List<String> checksums = association.get(key);

                // System.out.println(key);
                out.write(key + " " + checksums.size() + "\n");
                for (String checksum : checksums) {
                    // System.out.println(checksum + "  " + key);
                    out.write(checksum + "\n");
                }
            }
            out.write("/\n");
            for (String key: occurrences.keySet()) {
                Integer occurrence = occurrences.get(key);
                out.write(key + " " + occurrence + "\n");
            }
        } catch ( IOException e) {
            // e.printStackTrace();
        } finally {
            try {
                if ( out != null) {
                    out.close( );
                }
            } catch ( IOException e) {
                // e.printStackTrace();
            }
        }
        // System.out.println(FILE_PATH);
        storage.upload(FILE_PATH, "./" + FILE_PATH);
    }

    public static String convertToASCII(String checksum){
        String name = "";
        for (int i = 0; i < checksum.length(); i++) {
            name += (int)checksum.charAt(i);
        }
        return name;
    }

    public static void main(String args[]) {
        System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("https.proxyPort", "8000");
        System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("http.proxyPort", "8000");
        Storage storage = null;
        if (args[1].equals("local")) {
            storage = new LocalStorage();
        } else if (args[1].equals("azure")) {
            storage = new AzureStorage();
        } else if (args[1].equals("s3")) {
            storage = new AmazonS3Storage();
        }
        Index index = new Index(storage);
        if (args[0].equals("files")) {
            for (String filePath : index.getStoredFilePaths()) {
                System.out.println(filePath + " " + index.getChecksums(filePath).size());
            }
        } else if (args[0].equals("occurrences")) {
            Map<String, Integer> occurrences = index.getOccurrences();
            for (String key : occurrences.keySet()) {
                System.out.println(String.format("%-80s", key) + occurrences.get(key));
            }
        }
    }
}
