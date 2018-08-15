package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.databox.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataBox> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataBox> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataBox dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataBoxes corresponds to this schema. A list of
   * DataBoxes corresponds to this schema if the number of DataBoxes in the
   * list equals the number of columns in this schema, and if each DataBox has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataBoxes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataBox> values) throws SchemaException {
    // TODO: implement me!
      List types = this.getFieldTypes();

      if (values.size() != fields.size()) {
          throw new SchemaException("Not same size");
      }

      for (int i = 0; i < values.size(); i++) {
          DataBox current = values.get(i);
          DataBox toCheck = (DataBox) types.get(i);

          if (!toCheck.type().equals(current.type()) && current.getSize() != toCheck.getSize()) {
              throw new SchemaException("Not same size or type");
          }

      }
      Record ret = new Record(values);
      return ret;
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataBoxes'
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataBox. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
      List current = record.getValues();
      int counter = 0;
      for (int i = 0; i < current.size(); i++) {
          DataBox curr = (DataBox) current.get(i);
          counter += curr.getSize();
      }
      ByteBuffer bb = ByteBuffer.allocate(counter);

      for (int i = 0; i < current.size(); i++) {
          DataBox curr = (DataBox) current.get(i);
          bb.put(curr.getBytes());
      }

      return bb.array();
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
//      switch(input):
      List<DataBox> db = this.getFieldTypes();
      List<DataBox> values = new ArrayList<DataBox>();
      int start = 0;
      int end = 0;
      for (int i = 0; i < db.size(); i++) {

          DataBox currdatabox = db.get(i);

          end += currdatabox.getSize();

          byte[] curr = Arrays.copyOfRange(input, start, end);

          if (currdatabox.type().equals(DataBox.Types.BOOL)) {
              DataBox bool = new BoolDataBox(curr);
              values.add(bool);
          }

          if (currdatabox.type().equals(DataBox.Types.FLOAT)) {
              DataBox flo = new FloatDataBox(curr);
              values.add(flo);
          }

          if (currdatabox.type().equals(DataBox.Types.INT)) {
              DataBox integ = new IntDataBox(curr);
              values.add(integ);
          }

          if (currdatabox.type().equals(DataBox.Types.STRING)) {
              DataBox str = new StringDataBox(curr);
              values.add(str);
          }

          start += db.get(i).getSize();

      }

      Record ret = new Record(values);
      return ret;

  }

  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataBox> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataBox thisType = this.fieldTypes.get(i);
      DataBox otherType = otherSchema.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
