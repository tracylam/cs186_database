package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PNLJOperator extends JoinOperator {

  public PNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.PNLJ);
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new PNLJIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class PNLJIterator implements Iterator<Record> {
    /* TODO: Implement the PNLJIterator */
    /* Suggested Fields */
    private String leftTableName;
    private String rightTableName;
    private Iterator<Page> leftIterator;
    private Iterator<Page> rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Page leftPage;
    private Page rightPage;
    private byte[] leftHeader;
    private byte[] rightHeader;
    private int leftEntryNum;
    private int rightEntryNum;

    public PNLJIterator() throws QueryPlanException, DatabaseException {
      /* Suggested Starter Code: get table names. */
      if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
        this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
      } else {
        this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
        Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
        while (leftIter.hasNext()) {
          PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
        }
      }
      if (PNLJOperator.this.getRightSource().isSequentialScan()) {
        this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
      } else {
        this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
        PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
        Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
        while (rightIter.hasNext()) {
          PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
        }
      }
      /* TODO */

      leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
      rightIterator = PNLJOperator.this.getPageIterator(rightTableName);


      if (leftIterator.hasNext()) { //get left page header
        leftHeader = leftIterator.next().readBytes(); //assumes that only one page header per page iterator
      }

      if (rightIterator.hasNext()) { //get right page header
        rightHeader = rightIterator.next().readBytes(); //first iteration of rightIterator should be page zero thus header
      }

      if (leftIterator.hasNext()) { //call next to get first left page
        leftPage = leftIterator.next();
        leftRecord = getNextLeftRecordInPage();
      }

      if (rightIterator.hasNext()) { //call next to get first right page
        rightPage = rightIterator.next();
        rightRecord = getNextLeftRecordInPage();
      }

      nextRecord = null; //we don't know if there is a next record
      leftEntryNum = 0;
      rightEntryNum = 0;

    }

    public boolean hasNext() {
      /* TODO */
      if (this.nextRecord != null) {
        return true;
      }

        if (this.leftPage == null || this.rightPage == null) {
            return false;
        }

        if (this.leftRecord == null) {
            return false;
        }
        while (true) {
            if (this.rightRecord == null) { //checks if there is a rightrecord
                leftRecord = getNextLeftRecordInPage(); //if no right - check for next left record
                if (leftRecord == null) { //if the left record is also null go to next page
                 if (this.rightIterator.hasNext()) { //retrieves next page in right side
                     rightPage = rightIterator.next(); //get valid right page
                     rightEntryNum = 0; //reset the entry numbers bc new page
                     leftEntryNum = 0;

                     try {
                         rightHeader = getPageHeader(rightTableName, rightPage);
                     } catch (DatabaseException e) {
                         e.printStackTrace();
                         return false;
                     }

                     rightRecord = getNextRightRecordInPage();
                     leftRecord = getNextLeftRecordInPage();

                 }
                    else if (leftIterator.hasNext()) {
                     leftPage = leftIterator.next();
                     leftEntryNum = 0;
                     rightEntryNum = 0;

                     try {
                         rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
                         rightHeader = rightIterator.next().readBytes();

                         leftHeader = getPageHeader(leftTableName, leftPage);
                     } catch (DatabaseException e) {
                         e.printStackTrace();
                         return false;
                     }

                     rightPage = rightIterator.next();
                     leftRecord = getNextLeftRecordInPage();
                     rightRecord = getNextRightRecordInPage();

                 }
                    else {
                     return false;
                 }

                }
            } else {
                rightEntryNum = 0;
                rightRecord = getNextRightRecordInPage();
            }
//
//            while (getNextLeftRecordInPage() != null) {
//
//            }
        }

    }

    private Record getNextLeftRecordInPage() {
      /* TODO */
        try {
            while (this.leftEntryNum < PNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                byte b = leftHeader[this.leftEntryNum / 8];
                int bitOffset = 7 - (leftEntryNum % 8);
                byte mask = (byte) (1 << bitOffset);
                byte value = (byte) (b & mask);

                if (value != 0) {
                    int entSz = getEntrySize(leftTableName);
                    int offSet = getHeaderSize(leftTableName) + (entSz * leftEntryNum);
                    byte[] bytez = leftPage.readBytes(offSet, entSz);

                    Record ret = getLeftSource().getOutputSchema().decode(bytez);
                    leftEntryNum = leftEntryNum + 1;

                    return ret;
                }

                leftEntryNum += 1;

            }

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Record getNextRightRecordInPage() {
      /* TODO */
        try {
            while (this.rightEntryNum < PNLJOperator.this.getNumEntriesPerPage(rightTableName)) {
                byte b = rightHeader[this.rightEntryNum / 8];
                int bitOffset = 7 - (rightEntryNum % 8);
                byte mask = (byte) (1 << bitOffset);
                byte value = (byte) (b & mask);

                if (value != 0) {
                    int entSz = getEntrySize(rightTableName);
                    int offSet = getHeaderSize(rightTableName) + (entSz * rightEntryNum);
                    byte[] bytez = rightPage.readBytes(offSet, entSz);

                    Record ret = getLeftSource().getOutputSchema().decode(bytez);
                    rightEntryNum = rightEntryNum + 1;

                    return ret;
                }

                rightEntryNum += 1;

            }
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      /* TODO */
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
