package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;

/**
 * An inner node of a B+ tree. An InnerNode header contains an `isLeaf` flag
 * set to 0 and the page number of the first child node (or -1 if no child
 * exists). An InnerNode contains InnerEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {
    public static int headerSize = 5;       // isLeaf + pageNum of first child

    public InnerNode(BPlusTree tree) {
        super(tree, false);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 0);   // isLeaf = 0
        setFirstChild(-1);
    }

    public InnerNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, false);
        if (getPage().readByte(0) != (byte) 0) {
            throw new BPlusTreeException("Page is not Inner Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    public int getFirstChild() {
        return getPage().readInt(1);
    }

    public void setFirstChild(int val) {
        getPage().writeInt(1, val);
    }

    /**
     * Finds the correct child of this InnerNode whose subtree contains the
     * given key.
     *
     * @param key the given key
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key) {
        int childToTraverse = getFirstChild();
        List<BEntry> validEntries = getAllValidEntries();
        for (BEntry entry : validEntries) {
            if (key.compareTo(entry.getKey()) < 0) {
                break;
            } else {
                childToTraverse = entry.getPageNum();
            }
        }
        return childToTraverse;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */

    public InnerEntry insertBEntry(LeafEntry ent) {
        // innerEntry insert will call leafnodes insert, will have to find the child right, we are inserting a leaf entry. propagate down

        int pgNum = this.findChildFromKey(ent.getKey());
        BPlusNode childNode = getBPlusNode(getTree(), pgNum); //get subtree childnode
        InnerEntry result = childNode.insertBEntry(ent); //inserting entry into leaf

        if (result != null) { //if we actually have to add to inner entry
            if (!this.hasSpace()) {
                InnerEntry newInner = this.splitNode(result);

                return newInner;
            } else {
                List<BEntry> currEntries = this.getAllValidEntries();
                currEntries.add(result);
                Collections.sort(currEntries);
                this.overwriteBNodeEntries(currEntries);
            }
        }
        return null;
    }

    /**
     * Splits this InnerNode and returns the resulting InnerEntry to be
     * pushed/copied up to this InnerNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d entries.
     *
     * @param newEntry the BEntry that is being added to this InnerNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * InnerNode's parent as a result of this InnerNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        List<BEntry> LeftEntries = this.getAllValidEntries(); //this is the left node and its entries
        int leftSize = LeftEntries.size(); //old length
        int middleIndex = leftSize/2; //middle - will later be used to access middle of odd list

        InnerNode RightNode = new InnerNode(this.getTree());

        List<BEntry> RightEntries;

        LeftEntries.add(newEntry);
        Collections.sort(LeftEntries);


        InnerEntry toPush = (InnerEntry) LeftEntries.remove(middleIndex);

        InnerEntry ret = new InnerEntry(toPush.getKey(), RightNode.getPageNum()); //try with rightnode.getpgnum

        RightNode.setFirstChild(toPush.getPageNum());

        RightEntries = LeftEntries.subList(middleIndex, leftSize);
        LeftEntries = LeftEntries.subList(0, middleIndex);

        this.overwriteBNodeEntries(LeftEntries);
        RightNode.overwriteBNodeEntries(RightEntries);


        //pushing this node up but are you keeping references when adding

        return ret;
    }

}