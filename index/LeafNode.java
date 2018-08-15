package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A leaf node of a B+ tree. A LeafNode header contains an `isLeaf` flag set
 * to 1. A LeafNode contains LeafEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

    public static int headerSize = 1;       // isLeaf

    public LeafNode(BPlusTree tree) {
        super(tree, true);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 1);   // isLeaf = 1
    }

    public LeafNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, true);
        if (getPage().readByte(0) != (byte) 1) {
            throw new BPlusTreeException("Page is not Leaf Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    /**
     * Inserts a LeafEntry into this LeafNode.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this LeafNode's parent
     * as a result of this LeafNode being split, null otherwise
     */
    @Override
    public InnerEntry insertBEntry(LeafEntry ent) {
        if(!this.hasSpace()) { //first check if the leaf node is full or nah
            InnerEntry result = this.splitNode(ent); //node has been split, should exist two nodes with RNode with d+1 entries
            return result;
        } else {
            List<BEntry> currEntries = this.getAllValidEntries(); //get initial entries in node
            currEntries.add(ent); //add new entry
            Collections.sort(currEntries);
            this.overwriteBNodeEntries(currEntries); //update page with sorted
        }
        return null;
    }

    /**
     * Splits this LeafNode and returns the resulting InnerEntry to be
     * pushed/copied up to this LeafNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d+1 entries.
     *
     * @param newEntry the BEntry that is being added to this LeafNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * LeafNode's parent as a result of this LeafNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        //even though it says BEntry, we are passsing in a Leaf Entry when using this split

        // splits the current node bc full, returns key to be copied or pushed up, and L node d entries, R node d + 1 ent
        //so split is also doing some sort of insertion, assuming
        //newEntry is the entry that's making our node split so basically create new node and have old node move its shit
        List<BEntry> leftEntries = this.getAllValidEntries();
        int origSz = leftEntries.size()/2; // d
        leftEntries.add(newEntry);
        Collections.sort(leftEntries); // 2d + 1 entries
        BEntry middEnt = leftEntries.get(origSz);

        LeafNode newNode = new LeafNode(getTree()); //create new leaf node in same tree
        List<BEntry> rightEntries;

        int newSz = leftEntries.size(); //2d + 1

        rightEntries = leftEntries.subList(origSz, newSz);
        leftEntries = leftEntries.subList(0, origSz);

        newNode.overwriteBNodeEntries(rightEntries);

        this.overwriteBNodeEntries(leftEntries);

        InnerEntry ret = new InnerEntry(middEnt.getKey(), newNode.getPageNum());

        return ret;

    }


    /**
     * Creates an iterator of RecordIDs for all entries in this node.
     *
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scan() {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            rids.add(le.getRecordID());
        }

        return rids.iterator();
    }

    /**
     * Creates an iterator of RecordIDs whose keys are greater than or equal to
     * the given start value key.
     *
     * @param startValue the start value key
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scanFrom(DataBox startValue) {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            if (startValue.compareTo(le.getKey()) < 1) {
                rids.add(le.getRecordID());
            }
        }
        return rids.iterator();
    }

    /**
     * Creates an iterator of RecordIDs that correspond to the given key in the
     * current leafNode Page.
     *
     * @param key the search key
     * @return an iterator of RecordIDs
     */
    public Iterator<RecordID> scanForKey(DataBox key) {
        List<BEntry> validEntries = getAllValidEntries();
        List<RecordID> rids = new ArrayList<RecordID>();

        for (BEntry le : validEntries) {
            if (key.compareTo(le.getKey()) == 0) {
                rids.add(le.getRecordID());
            }
        }
        return rids.iterator();
    }

    public boolean containsKey(DataBox key) {
        List<BEntry> validEntries = getAllValidEntries();

        for (BEntry le : validEntries) {
            if (key.compareTo(le.getKey()) == 0) {
                return true;
            }
        }
        return false;

    }
}
