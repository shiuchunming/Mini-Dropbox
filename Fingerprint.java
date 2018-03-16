import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class Fingerprint {

    public int windowSize;
    public int baseSize;
    public int modulus;
    public int maxChunkSize;
    public int avgChunkSize;
    public int minChunkSize;

    private byte[] pattern;
    public int sizeOfPattern;
    public int curInd;
    public int lengthOfFingerprint;
    public int previous;
    public boolean exceptionCase;

    public int fingerPrintInterest;
    public int anchorOffset;
    public int anchorLen;

    private boolean recalculate;

    private static MessageDigest md;
    private List<String> chunksList;


    public Fingerprint(byte[] _t, int _minChunkSize, int _avgChunkSize, int _maxChunkSize, int _baseSize, int _length_of_file) {
        this.windowSize = _minChunkSize;
        this.baseSize = _baseSize;
        this.modulus = _avgChunkSize;

        this.minChunkSize = _minChunkSize;
        this.avgChunkSize = _avgChunkSize;
        this.maxChunkSize = _maxChunkSize;

        this.pattern = _t;
        this.sizeOfPattern = _length_of_file;
        this.lengthOfFingerprint = this.sizeOfPattern - this.windowSize + 1;

        this.curInd = 0;
        this.previous = 0;


        this.fingerPrintInterest = 0;
        this.anchorOffset = 0;
        this.anchorLen = 0;

        if (this.sizeOfPattern < this.minChunkSize) {
            System.out.println("Pattern size lower than minChunkSize");
        }


        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        this.chunksList = new ArrayList<String>();
    }

    public int powerOf(int base, int exp, int q){
        if (exp == 0) {
            return 1;
        }
        int temp = powerOf(base, exp/2, q);
        if (exp % 2 == 0){
            return (temp%q * temp%q)%q;
        }else{
            return ((base%q * temp%q)%q * temp%q)%q;
        }
    }

    private int mod(int val, int q) {
        return (val % q + q) % q;
    }

    public int calculate() {
        int index = this.curInd;
        int fingerPrint = 0;
        if (this.curInd == 0 || recalculate == true) {
            for (int i = 0; i < this.windowSize; i++) {
                fingerPrint += (pattern[curInd + i]) * this.powerOf(baseSize, this.windowSize - i - 1, this.modulus);
            }
            fingerPrint = mod(fingerPrint, this.modulus);
            this.recalculate = false;
        } else if (this.curInd < this.lengthOfFingerprint) {
            fingerPrint =
                    (
                            (
                                    (int) (baseSize * (this.previous - (this.powerOf(baseSize, this.windowSize - 1, this.modulus) * (pattern[index - 1]))))
                            )
                                    + pattern[index + this.windowSize - 1]
                    );
            fingerPrint = mod(fingerPrint, this.modulus);

        } else {
            System.out.println("Cur index out of lengthOfFingerprint range");
            return -10000000;
        }
        this.previous = fingerPrint;
        this.curInd++;
        this.anchorLen++;

        // System.out.println("Cur fingerPrint -----------------------: " + fingerPrint );
        return fingerPrint;
    }

    public void resetAnchorLen() {
        this.anchorLen = 0;
        this.curInd = this.curInd + this.windowSize - 1;
        this.anchorOffset = this.curInd;
        this.recalculate = true;
    }


    public int getAnchorLen() {
        int rfp = -1;
        int _anchorLen = 0;
        if(this.sizeOfPattern > this.windowSize){
            if ((this.anchorOffset + this.minChunkSize) < this.lengthOfFingerprint) {
                for (int i = this.anchorOffset; i < this.anchorOffset + this.maxChunkSize - this.windowSize + 1 && i < this.lengthOfFingerprint; i++) {
                    rfp = this.calculate();
                    if ( (rfp & 0xFF) == 0) {
                        _anchorLen = this.anchorLen + this.windowSize - 1;
                        this.resetAnchorLen();
                        return _anchorLen;
                    }
                }
                _anchorLen = this.anchorLen + this.windowSize - 1;
                this.resetAnchorLen();
                return _anchorLen;


            } else {
                // System.out.println("Chunk too small curInd: " + curInd);
                _anchorLen = this.sizeOfPattern - this.curInd;
                this.anchorLen = 0;
                this.anchorOffset = this.lengthOfFingerprint;
                this.recalculate = false;
                this.curInd = this.sizeOfPattern;
                return _anchorLen;
            }
      }else{
        this.anchorOffset = this.lengthOfFingerprint;
        this.recalculate = false;
        this.curInd = this.sizeOfPattern;
        return this.sizeOfPattern;
      }
    }


    public String checksum(int offset, int len) {
        byte[] hash;
        md.update(this.pattern, offset, len);
        hash = md.digest();
        return Base64.getEncoder().encodeToString(hash);
    }

    public static void main(String[] args) {
        byte[] t = Storage.read(args[0]);
        int minChunkSize = Integer.parseInt(args[1]);
        int avgChunkSize = Integer.parseInt(args[2]);
        int maxChunkSize = Integer.parseInt(args[3]);

        Fingerprint fingerprint = new Fingerprint(t, minChunkSize, avgChunkSize, maxChunkSize, 10, t.length);

        int len = 0;
        int rfp = -1;
        String checksum = "";
        System.out.println("Len of file : " + t.length);
        for (int i = 0; i < fingerprint.sizeOfPattern; i++) {
            len = fingerprint.getAnchorLen();
            rfp = fingerprint.previous;
            System.out.println("Cur Start " + i);
            System.out.println("FingerPrint : " + rfp);
            System.out.println("Anchor len : " + len);
            try {
              checksum = fingerprint.checksum(i, len);
              System.out.println("Checksum " + i + " : " + checksum);
            } catch (Exception e){
              System.out.println("exception on check sum: " + (i + len));
            }
            i = fingerprint.curInd - 1;
            System.out.println("-----------------------------------------------");
            System.out.println("Cur end " + i + " and the lengthOfFingerprint: " + fingerprint.lengthOfFingerprint);
        }
    }

    public List<String> getChunkID() {
        return this.chunksList;
    }

}
