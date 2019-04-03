import java.util.*;
import java.nio.file.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import com.google.gson.Gson;

/* JSON Format

{"file":
  [
     {"name":"MyFile",
      "size":128000000,
      "pages":
      [
         {
            "guid":11,
            "size":64000000
         },
         {
            "guid":13,
            "size":64000000
         }
      ]
      }
   ]
} 
*/

public class DFS {

    /**
     * This class is for Meta Page
     */
    public class PagesJson {
        Long guid;
        Long size;
        int pageNumber;

        public PagesJson(long guid, long size) {
            this.guid = guid;
            this.size = size;
        }

        public long getPageNumber() {
            return size;
        }

        public long getGUID() {
            return guid;
        }
        // getters
        // setters
    };

    /**
     * This class is for Meta File
     */
    public class FileJson {
        String name;
        Long size;
        ArrayList<PagesJson> pages;

        public FileJson(String name) {
            this.name = name;
        }

        public void addPage(long guid) {
            pages.add(new PagesJson(pages.size() + 1, guid));
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getNumOfPages() {
            return pages.size();
        }

        public PagesJson getPage(int pageNumber) {
            return pages.get(pageNumber - 1);
        }

        public void printListOfPages() {
            System.out.printf("\n%-5s%-15s%-15s\n", "", "Page Number", "GUID");
            for (int i = 0; i < pages.size(); i++) {
                PagesJson temp = pages.get(i);

                System.out.printf("%-5s%-15s%-15d\n", "", temp.getPageNumber(), temp.getGUID());
            }
            System.out.println("");
        }
        // getters
        // setters
    };

    /**
     * This class is for Meta Data
     */
    public class FilesJson {
        // List<FileJson> file;
        ArrayList<FileJson> file = new ArrayList<FileJson>();

        public FilesJson() {

        }

        /**
         * Creates a new file within a FilesJson
         * 
         * @param fileName Name of the new file
         */
        public void addFile(String fileName) {
            FileJson newFile = new FileJson(fileName);
            file.add(newFile);
        }

        /**
         * A method used on FilesJson objects, to list all file names along with the
         * number of pages.
         */
        public void printListOfFiles() {
            System.out.printf("\n%-15s%-15s\n", "Filename", "Number of Pages");
            for (int i = 0; i < file.size(); i++) {
                FileJson temp = file.get(i);

                System.out.printf("%-15s%-15d\n", temp.getName(), temp.getNumOfPages());

                if (temp.getNumOfPages() > 0)
                    temp.printListOfPages();
            }
            System.out.println("");
        }

        /**
         * This method cycles through the names in a FilesJson
         * 
         * @param filename The name of the file the method is looking for.
         * @return If the name is found, that FileJson will be returned. Otherwise a
         *         null will be returned.
         */
        public FileJson getFile(String filename) {
            for (int i = 0; i < file.size(); i++) {
                FileJson temp = file.get(i);

                if (temp.getName().equals(filename)) {
                    return temp;
                }
            }
            return null;
        }

        /**
         * This iterates through a FilesJson, to look the existence of a desired
         * FileJson
         * 
         * @param filename Name of the desired FileJson.
         * @return True if a match for filename is found. Otherwise a false will be
         *         returned.
         */
        public boolean fileExists(String filename) {
            for (int i = 0; i < file.size(); i++) {
                FileJson temp = file.get(i);

                if (temp.getName().equals(filename)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Creates a ListIterator of the FilesJson, and iterates to look for the desired
         * FileJson and deletes it if it exists.
         * 
         * @param filename Name of the desired FileJson.
         */
        public void deleteFile(String filename) {
            ListIterator<FileJson> listIterator = file.listIterator();

            while (listIterator.hasNext()) {
                FileJson temp = listIterator.next();

                if (temp.getName().equals(filename))
                    listIterator.remove();
            }
        }

        public void clear() {
            file.clear();
        }
        // getters
        // setters
    };

    int port;
    Chord chord;

    /**
     * Generates a encrypted guid based off the inputted string, using the MD5
     * algorithm.
     * 
     * @param objectName Name of the object being encrypted
     * @return long guid
     */
    private long md5(String objectName) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());
            BigInteger bigInt = new BigInteger(1, m.digest());
            return Math.abs(bigInt.longValue());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();

        }
        return 0;
    }

    /**
     * ???
     * 
     * @param port
     * @throws Exception
     */
    public DFS(int port) throws Exception {

        this.port = port;
        long guid = md5("" + port);
        chord = new Chord(port, guid);
        Files.createDirectories(Paths.get(guid + "/repository"));
        Files.createDirectories(Paths.get(guid + "/tmp"));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                chord.leave();
            }
        });

    }

    /**
     * Joins the cord
     * 
     * @param Ip
     * @param port
     * @throws Exception
     */
    public void join(String Ip, int port) throws Exception {
        chord.joinRing(Ip, port);
        chord.print();
    }

    /**
     * leave the chord
     *
     */
    public void leave() throws Exception {
        chord.leave();
    }

    /**
     * print the status of the peer in the chord
     *
     */
    public void print() throws Exception {
        chord.print();
    }

    /**
     * Reads the metadata from the chord
     * 
     * @return A FilesJson object from that chord
     * @throws Exception Checks for existence of a FilesJson object
     */
    public FilesJson readMetaData() throws Exception {
        FilesJson filesJson = null;
        try {
            Gson gson = new Gson();
            long guid = md5("Metadata");
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            RemoteInputFileStream metadataraw = peer.get(guid);
            metadataraw.connect();
            Scanner scan = new Scanner(metadataraw);
            scan.useDelimiter("\\A");
            String strMetaData = scan.next();
            System.out.println(strMetaData);
            filesJson = gson.fromJson(strMetaData, FilesJson.class);
        } catch (NoSuchElementException ex) {
            filesJson = new FilesJson();
        }
        return filesJson;
    }

    /**
     * writeMetaData write the metadata back to the chord
     * 
     * @param filesJson
     * @throws Exception
     */
    public void writeMetaData(FilesJson filesJson) throws Exception {
        long guid = md5("Metadata");
        ChordMessageInterface peer = chord.locateSuccessor(guid);

        Gson gson = new Gson();
        peer.put(guid, gson.toJson(filesJson));
    }

    /**
     * Changes the name of a file.
     * 
     * @param oldName Current name of file
     * @param newName New name of the file
     * @throws Exception
     */
    public void move(String oldName, String newName) throws Exception {

        // Setting temp JsonObject
        FilesJson md = readMetaData();
        if (md.fileExists(oldName)) {
            FileJson metafile = md.getFile(oldName);
            metafile.setName(newName);
            writeMetaData(md);
        } else
            System.out.println("That file does not exist. Try again.");

    }

    /**
     * Creates an empty file.
     * 
     * @param fileName Name of the newly created file.
     * @throws Exception
     */
    public void create(String fileName) throws Exception {
        FilesJson metaData = new FilesJson();
        metaData.addFile(fileName);
        writeMetaData(metaData);
    }

    /**
     * Prints a list of files in the FilesJson
     * 
     * @return Prints files from "printListOfFiles"
     * @throws Exception
     */
    public void lists() throws Exception {
        FilesJson metaData = readMetaData();
        if (metaData.file.size() > 0 || metaData == null) {
            FilesJson metadata = readMetaData();
            metadata.printListOfFiles();
        } else
            System.out.println("No files found in metadata.");
    }

    /**
     * Deletes a file from a FilesJson, and updates it.
     * 
     * @param fileName Name of file to be deleted
     * @throws Exception
     */
    public void delete(String fileName) throws Exception {
        FilesJson md = readMetaData();
        if (md.fileExists(fileName)) {
            FileJson metafile = md.getFile(fileName);
            if (metafile.getNumOfPages() > 0) {
                for (int i = 0; i < metafile.getNumOfPages(); i++) {
                    PagesJson page = metafile.pages.get(i);
                    long guid = page.getGUID();
                    ChordMessageInterface peer = chord.locateSuccessor(guid);
                    peer.delete(guid);
                }
            }
            md.deleteFile(fileName);
            writeMetaData(md);
        } else
            System.out.println("That file does not exist. Try again.");
    }

    /**
     * Reads, and prints, the file's metadata.
     * 
     * @param fileName   Name of the desired file.
     * @param pageNumber Page number where file is located
     * @return Prints out the metadata of that file
     * @throws Exception Checks to see if file exists.
     */
    public RemoteInputFileStream read(String fileName, int pageNumber) throws Exception {
        FilesJson md = readMetaData();
        if (md.fileExists(fileName)) {
            FileJson metafile = md.getFile(fileName);
            PagesJson page = metafile.getPage(pageNumber);
            long guid = page.getGUID();
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            InputStream metadataraw = peer.get(guid);

            int content;
            while ((content = metadataraw.read()) != 0) {
                System.out.print((char) content);
            }
            System.out.println("");
        } else
            System.out.println("That file could not be located...");
        return null;
    }

    /**
     * Append
     * 
     * @param filename
     * @param data
     * @throws Exception Checks to see if file exists.
     */
    public void append(String filename, RemoteInputFileStream data) throws Exception {
        FilesJson md = readMetaData();
        if (md.fileExists(filename)) {
            long guid = md5(filename);

            FilesJson real_file = new FilesJson(filename);
            ChordMessageInterface peer = chord.locateSuccessor(guid);
            peer.put(guid, real_file);

            FileJson metafile = md.getFile(filename);
            metafile.addPage(guid);
            writeMetaData(md);
        } else
            System.out.println("That file could not be located...");
    }

}
