package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    //name to unique pageID
    ConcurrentHashMap<String, Integer>  catalogName;
    //unique pageID to file
    ConcurrentHashMap<Integer, DbFile> catalogID;
    //unique pageID to primary key
    ConcurrentHashMap<Integer, String> catalogPKey;

    //inverse maps
    ConcurrentHashMap<Integer, String> reverseCatName;
    ConcurrentHashMap<DbFile, Integer> reverseCatID;


    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        this.catalogName = new ConcurrentHashMap<String, Integer>();
        this.catalogID = new ConcurrentHashMap<Integer, DbFile>();
        this.catalogPKey = new ConcurrentHashMap<Integer, String>();
        this.reverseCatID = new ConcurrentHashMap<DbFile, Integer>();
        this.reverseCatName = new ConcurrentHashMap<Integer, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
       /* if(catalogName.containsKey(name)){
            catalogID.remove(catalogName.get(name));
            catalogName.remove(name);
        }*/
        catalogID.put(file.getId(), file);
        catalogName.put(name, file.getId());
        catalogPKey.put(file.getId(), pkeyField);
        //TODO add to inverse tables
    }

    public void addTable(DbFile file, String name) {

        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {

        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        if(name != null && catalogName.containsKey(name)){
            return catalogName.get(name).intValue();

        }else {
            throw new NoSuchElementException("That name is not in the catalog.");
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // DbFile class has method getTupleDesc().
        //use tableID to find file in catalogID and call method
        if(!catalogID.containsKey(tableid)){
            throw new NoSuchElementException("That tableid is not in the catalog.");
        }
        DbFile file = catalogID.get(tableid);
        return file.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        //use the code from getTupleDesc above
        if(!catalogID.containsKey(tableid)){
            throw new NoSuchElementException("That table is not in the catalog.");
        }
        return catalogID.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        if(!catalogPKey.containsKey(tableid)){
            throw new NoSuchElementException();
        }
        return catalogPKey.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        //iterates over all the tableID's... Could use any of catalog maps...
            //ASK: which one is the best one to use? How will this iterator be used?
        return catalogID.keySet().iterator();
    }

    public String getTableName(int id) {
        //table name is the key, id is the value
        //use .entrySet() to get key-value pairs
        //search through all of catalogName

        //TODO use inverse table
        for(Map.Entry<String, Integer> entry : catalogName.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            //if the value == id then we want to return that key and stop
            if (value == id) {
                return key;
            }
        }
        //can't do if statement or method will want another return statement, just return a string stating the same
        return "The table does not exist.";
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        catalogName.clear();
        catalogID.clear();
        catalogPKey.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

