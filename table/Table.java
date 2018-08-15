package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import java.util.NoSuchElementException;
import java.util.Iterator;
import java.io.Closeable;

/**
 * A database table. Allows the user to add, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 *
 * Properties:
 * `schema`: the Schema (column names and column types) for this table
 * `freePages`: a set of page numbers that correspond to allocated pages with free space
 * `stats`: the TableStats for this table
 * `allocator`: the PageAllocator for this table
 * `tableName`: name of this table
 * `numEntriesPerPage`: number of records a data page of this table can hold
 * `pageHeaderSize`: physical size (in bytes) of a page header slot bitmap
 * `numRecords`: number of records currently contained in this table
 */
public class Table implements Iterable<Record>, Closeable {
  public static final String FILENAME_PREFIX = "db";
  public static final String FILENAME_EXTENSION = ".table";

  private Schema schema;
  private TreeSet<Integer> freePages;

  private TableStats stats;

  private PageAllocator allocator;
  private String tableName;

  private int numEntriesPerPage;
  private int pageHeaderSize;
  private long numRecords;

  public Table(String tableName) {
    this(tableName, FILENAME_PREFIX);
  }

  public Table(String tableName, String filenamePrefix) {
    this.tableName = tableName;

    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, false);
    this.readHeaderPage();

    this.stats = new TableStats(this.schema);

    this.freePages = new TreeSet<Integer>();
    this.setEntryCounts();
    Iterator<Page> pIter = this.allocator.iterator();
    pIter.next();

    long freshCountRecords = 0;

    while(pIter.hasNext()) {
      Page p = pIter.next();

      // add all records in this page to TableStats
      int entryNum = 0;
      byte[] header = this.readPageHeader(p);
      while (entryNum < this.getNumEntriesPerPage()) {
        byte b = header[entryNum/8];
        int bitOffset = 7 - (entryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = this.schema.getEntrySize();

          int offset = this.pageHeaderSize + (entrySize * entryNum);
          byte[] bytes = p.readBytes(offset, entrySize);

          Record record = this.schema.decode(bytes);
          this.stats.addRecord(record);
        }

        entryNum++;
      }

      if (spaceOnPage(p)) {
        this.freePages.add(p.getPageNum());
      }

      freshCountRecords += numValidEntries(p);
    }

    this.numRecords = freshCountRecords;
  }

  public Table(Schema schema, String tableName) {
    this(schema, tableName, FILENAME_PREFIX);
  }

  /**
   * This constructor is used for creating a table in some specified directory.
   *
   * @param schema the schema for this table
   * @param tableName the name of the table
   * @param filenamePrefix the prefix select the table's files will be created
   */
  public Table(Schema schema, String tableName, String filenamePrefix) {
    this.schema = schema;
    this.tableName = tableName;
    this.stats = new TableStats(this.schema);

    this.freePages = new TreeSet<Integer>();
    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, true);

    this.setEntryCounts();

    this.writeHeaderPage();
  }

  public void close() {
    allocator.close();
  }

  public Iterator<Record> iterator() {
      return new TableIterator();
  }

  public Iterator<Page> pageIterator() {
    return this.allocator.iterator();
  }

  /**
   * Adds a new record to this table. The record should be added to the first
   * free slot of the first free page if one exists, otherwise a new page should
   * be allocated and the record should be placed in the first slot of that
   * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
   * Make sure to update this.stats, this.freePages, and this.numRecords as
   * necessary.
   *
   * @param values the values of the record being added
   * @return the RecordID of the added record
   * @throws DatabaseException if the values passed in to this method do not
   *         correspond to the schema of this table
   */


  public RecordID addRecord(List<DataBox> values) throws DatabaseException {
      Record rec;
      try {
          rec = schema.verify(values);
      } catch (SchemaException se) {
          throw new DatabaseException(se.getMessage());
      }

      int freePgNum;
      Page freePg = null;
      boolean valid = false;

      int slotPos = 0;

      byte[] sz = schema.encode(rec);

      while(valid != true) {
          if (freePages.isEmpty()) {
              freePgNum = allocator.allocPage();
              freePg = allocator.fetchPage(freePgNum);
              freePages.add(freePgNum);
          } else {
              freePgNum = freePages.first();
              freePg = allocator.fetchPage(freePgNum);
          }

          slotPos = 0;

          byte[] pgHeader = readPageHeader(freePg);

          for (int i = 0; i < pgHeader.length; i++) {
              byte b = pgHeader[i];
              if (b != (byte) 0xFF) {
                  for (int j = 7; 7 >= 0; j--) {
                      byte mask = (byte) (1 << j);
                      if ((b & mask) == (byte) 0) {
                          valid = true;
                          break;
                      }
                      slotPos += 1;
                  }
              }
              else {
                  slotPos += 8;
              }

              if (valid) {
                  break;
              }
          }
          if (valid != true) {
              freePages.remove(freePgNum);

              //if you didn't find any free space.
          }
      }

      int offset = pageHeaderSize + (slotPos * schema.getEntrySize());
      int numBytes = sz.length;

      freePg.writeBytes(offset, numBytes, sz);
      writeBitToHeader(freePg, slotPos, (byte) 1);
      this.numRecords = numRecords + 1;
      this.stats.addRecord(rec);

      RecordID ret = new RecordID(freePg.getPageNum(), slotPos);
      return ret;

  }

    private int getFreeRecordPos(byte[] pgHeader) {

        int slotPos = 0;

        for (int i = 0; i < pgHeader.length; i++) {
            byte b = pgHeader[i];
            if (b != (byte) 0xFF) {
                for (int j = 7; 7 >= 0; j--) {
                    byte transform = (byte) (1 << j);
                    if ((b & transform) == (byte) 0) {
                        break;
                    }
                    slotPos += 1;
                }
            } else {
                slotPos += 8;
            }
        }
        return slotPos;
    }

  /**
   * Deletes the record specified by rid from the table. Make sure to update
   * this.stats, this.freePages, and this.numRecords as necessary.
   *
   * @param rid the RecordID of the record to delete
   * @return the Record referenced by rid that was removed
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record deleteRecord(RecordID rid) throws DatabaseException {
      try {
          checkRecordIDValidity(rid);
      } catch (DatabaseException db) {
          throw new DatabaseException(db.getMessage());
      }


      int pgNum = rid.getPageNum();
      Page pg = allocator.fetchPage(pgNum);
      Record oldRecord = getRecord(rid);

      writeBitToHeader(pg, rid.getEntryNumber(), (byte) 0);
      this.stats.removeRecord(oldRecord);
      this.freePages.add(pgNum);

      numRecords = numRecords - 1;

      return oldRecord;
  }

  /**
   * Retrieves a record from the table.
   *
   * @param rid the RecordID of the record to retrieve
   * @return the Record referenced by rid
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record getRecord(RecordID rid) throws DatabaseException {
      try {
          checkRecordIDValidity(rid);
      } catch (DatabaseException db) {
          throw new DatabaseException(db.getMessage());
      }


      int pgIndex = rid.getPageNum();
      Page pg = allocator.fetchPage(pgIndex);

      int entrySize = schema.getEntrySize();
      int offset = this.pageHeaderSize + (entrySize * rid.getEntryNumber());

      byte[] bytes = pg.readBytes(offset, entrySize);

      return schema.decode(bytes);

  }

  /**
   * Updates an existing record with new values and returns the old version of the record.
   * Make sure to update this.stats as necessary.
   *
   * @param values the new values of the record
   * @param rid the RecordID of the record to update
   * @return the old version of the record
   * @throws DatabaseException if rid does not correspond to a valid record or
   *         if the values do not correspond to the schema of this table
   */
  public Record updateRecord(List<DataBox> values, RecordID rid) throws DatabaseException {
      try {
          checkRecordIDValidity(rid);
      } catch (DatabaseException db) {
          throw new DatabaseException(db.getMessage());
      }

      Record newRecord;
      try {
          newRecord = schema.verify(values);
      } catch (SchemaException se) {
          throw new DatabaseException(se.getMessage());
      }

      int pgNum = rid.getPageNum();
      Page pg = allocator.fetchPage(pgNum);

      int entrySize = schema.getEntrySize();
      int offset = this.pageHeaderSize + (entrySize * rid.getEntryNumber());

      Record oldRecord = getRecord(rid);

      byte[] toWrite = schema.encode(newRecord);

      pg.writeBytes(offset, toWrite.length, toWrite);

      this.stats.removeRecord(oldRecord);
      this.stats.addRecord(newRecord);

      return oldRecord;

  }

  public int getNumEntriesPerPage() {
    return this.numEntriesPerPage;
  }

  public int getNumDataPages() {
    return this.allocator.getNumPages() - 1;
  }

  public long getNumRecords() {
    return this.numRecords;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public TableStats getStats() { return this.stats; }

  /**
   * Checks whether a RecordID is valid or not. That is, check to see if the slot
   * in the page specified by the RecordID contains a valid record (i.e. whether
   * the bit in the slot bitmap is set to 1).
   *
   * @param rid the record id to check
   * @return true if rid corresponds to a valid record, otherwise false
   * @throws DatabaseException if rid does not reference an existing data page slot
   */
  private boolean checkRecordIDValidity(RecordID rid) throws DatabaseException {

      Page testPg;
      try {
          testPg = allocator.fetchPage(rid.getPageNum());
      } catch (PageException p) {
          throw new DatabaseException("Not valid page");
      }

      if (rid == null) {
          throw new DatabaseException("null record");
      }

      if (rid.getPageNum() == 0) {
          throw new DatabaseException("This Page is for the table header");
      }

      byte[] pgHeader = readPageHeader(testPg);
      int entryNum = rid.getEntryNumber();

      if (entryNum >= numEntriesPerPage) {
          throw new DatabaseException("Entry number is out of the page's bounds");
      }

      int byteOffset = entryNum / 8;
      int bitOffset = 7 - (entryNum % 8);
      byte mask = (byte) (1 << bitOffset);
      byte value = (byte) (pgHeader[byteOffset] & mask);

      if (value == 0) {
          throw new DatabaseException("record no longer there");
      }

      return value != 0;

  }

  /**
   * Based on the Schema known to this table, calculates the number of record
   * entries a data page can hold and the size (in bytes) of the page header.
   * The page header only contains the slot bitmap and takes up no other space.
   * For ease of calculations and to prevent header byte splitting, ensure that
   * `numEntriesPerPage` is a multiple of 8 (this may waste some space).
   *
   * Should set this.pageHeaderSize and this.numEntriesPerPage.
   */
  private void setEntryCounts() {
      List<DataBox> db = this.getSchema().getFieldTypes();
      Record rec = new Record(db);
      byte[] bytez = this.getSchema().encode(rec);

      int recordBits = ((bytez.length * 8) + 1);
      int pgBits = Page.pageSize * 8;

      int numentries = pgBits/recordBits;

      numentries = numentries - (numentries % 8);

      this.numEntriesPerPage = numentries;

      this.pageHeaderSize = numentries/8;

  }

  /**
   * Checks if there is any free space on the given page.
   *
   * @param p the page to check
   * @return true if there exists free space, otherwise false
   */
  private boolean spaceOnPage(Page p) {
    byte[] header = this.readPageHeader(p);

    for (byte b : header) {
      if (b != (byte) 0xFF) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks how many valid record entries are in the given page.
   *
   * @param p the page to check
   * @return number of record entries in p
   */
  private int numValidEntries(Page p) {
    byte[] header = this.readPageHeader(p);
    int count = 0;

    for (byte b : header) {
      for (int mask = 0x01; mask != 0x100; mask <<= 1) {
        if ((b & (byte) mask) != 0) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Utility method to write the header page of the table. The only information written into
   * the header page is the table's schema.
   */
  private void writeHeaderPage() {
    int numBytesWritten = 0;
    Page headerPage = this.allocator.fetchPage(this.allocator.allocPage());

    assert(0 == headerPage.getPageNum());

    List<String> fieldNames = this.schema.getFieldNames();
    headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldNames.size()).array());
    numBytesWritten += 4;

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldName.length()).array());
      numBytesWritten += 4;
    }

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, fieldName.length(), fieldName.getBytes(Charset.forName("UTF-8")));
      numBytesWritten += fieldName.length();
    }

    for (DataBox field : this.schema.getFieldTypes()) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.type().ordinal()).array());
      numBytesWritten += 4;

      if (field.type().equals(DataBox.Types.STRING)) {
        headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.getSize()).array());
        numBytesWritten += 4;
      }
    }
  }

  /**
   * Utility method to read the header page of the table.
   */
  private void readHeaderPage() {
    int numBytesRead = 0;
    Page headerPage = this.allocator.fetchPage(0);

    int numFields = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
    numBytesRead += 4;

    List<Integer> fieldNameLengths = new ArrayList<Integer>();
    for (int i = 0; i < numFields; i++) {
      fieldNameLengths.add(ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt());
      numBytesRead += 4;
    }

    List<String> fieldNames = new ArrayList<String>();
    for (int fieldNameLength : fieldNameLengths) {
      byte[] bytes = headerPage.readBytes(numBytesRead, fieldNameLength);

      fieldNames.add(new String(bytes, Charset.forName("UTF-8")));
      numBytesRead += fieldNameLength;
    }

    List<DataBox> fieldTypes = new ArrayList<DataBox>();
    for (int i = 0; i < numFields; i++) {
      int ordinal = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
      DataBox.Types type = DataBox.Types.values()[ordinal];
      numBytesRead += 4;

      switch(type) {
        case INT:
          fieldTypes.add(new IntDataBox());
          break;
        case STRING:
          int len = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
          numBytesRead += 4;

          fieldTypes.add(new StringDataBox(len));
          break;
        case BOOL:
          fieldTypes.add(new BoolDataBox());
          break;
        case FLOAT:
          fieldTypes.add(new FloatDataBox());
          break;
      }
    }

    this.schema = new Schema(fieldNames, fieldTypes);

  }

  /**
   * Utility method to write a particular bit into the header of a particular page.
   *
   * @param page the page to modify
   * @param entryNum the header slot to modify
   * @param value the value of the bit to write (should either be 0 or 1)
   */
  private void writeBitToHeader(Page page, int entryNum, byte value) {
    byte[] header = this.readPageHeader(page);
    int byteOffset = entryNum / 8;
    int bitOffset = 7 - (entryNum % 8);

    if (value == 0) {
      byte mask = (byte) ~((1 << bitOffset));

      header[byteOffset] = (byte) (header[byteOffset] & mask);
      page.writeBytes(0, this.pageHeaderSize, header);
    } else {
      byte mask = (byte) (1 << bitOffset);

      header[byteOffset] = (byte) (header[byteOffset] | mask);
    }

    page.writeBytes(0, this.pageHeaderSize, header);
  }

  /**
   * Read the slot header of a page.
   *
   * @param page the page to read from
   * @return a byte[] with the slot header
   */
  public byte[] readPageHeader(Page page) {
    return page.readBytes(0, this.pageHeaderSize);
  }

  public int getPageHeaderSize() {
    return this.pageHeaderSize;
  }

  public int getEntrySize()  {
    return this.schema.getEntrySize();
  }

  public int getNumPages() { return this.allocator.getNumPages(); }

  /**
   * An implementation of Iterator that provides an iterator interface over all
   * of the records in this table.
   */
  private class TableIterator implements Iterator<Record> {
      private Iterator<Page> pgIter;
      private Page currPg = null;
      private byte[] currHeader;
      private long numRecordsSeen;
      int entry;

    public TableIterator() {
        pgIter = allocator.iterator();

        this.entry = 0;

        Page PageHeader = pgIter.next();

        if (pgIter.hasNext()) {
            currPg = pgIter.next();
            currHeader = readPageHeader(currPg);
        }

        //construct a table iterator should be going through the page iterator and finding the valid records on that page
        //also use record iterator to go through the pages given through page iterator

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
        if (currPg != null && numRecordsSeen < numRecords) {
            return true;
        }
        if (currPg == null) {
            return false;
        }
        if (numRecordsSeen > numRecords) {
            return false;
        }
        return false;

    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {

        while (this.hasNext()) {
            while (entry < numEntriesPerPage) {
                int byteOffset = entry / 8;
                byte bytez = currHeader[byteOffset];
                int bitOffset = 7 - (this.entry % 8);
                byte mask = (byte) (1 << bitOffset);
                byte value = (byte) (bytez & mask);

                if (value != 0) {
                    int offset = pageHeaderSize + (schema.getEntrySize() * entry);
                    byte[] bytes = currPg.readBytes(offset, schema.getEntrySize());
                    this.numRecordsSeen++;
                    entry = entry + 1;

                    return schema.decode(bytes);
                }
                entry = entry + 1;
            }

            if (this.hasNext()) {
                entry = 0;
                currPg = pgIter.next();
                currHeader = readPageHeader(currPg);
            }
        }

        throw new NoSuchElementException();

    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
