package CSCI485ClassProject;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.*;

import javax.swing.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }

  // your code here
  private String tableName;
  private ComparisonOperator operator;
  private Mode mode;
  private boolean isUsingIndex;



  private Transaction tx;
  private Database db;

  private TableManagerImpl tableManager;

  private RecordTransformer recordTransformer;

  private AsyncIterator<KeyValue> iterator;

  private DirectorySubspace tableDirectory;

  private List<String> recordAttributeStorePath;

  private Direction direction = Direction.UNSET;
  enum Direction {
    FIRST_2_LAST,
    LAST_2_FIRST,
    UNSET
  }

  private CursorStatus cursorStatus = CursorStatus.UNINITIALIZED;
  enum CursorStatus {
    UNINITIALIZED,
    DIRECTION_SET,
    ITERATOR_INITIALIZED,
    EOF,
    COMMITTED,
    ERROR
  }

  private Tuple currentPrimaryKeyValueTuple = null;
  private Record currentRecord = null;

  private Function<Record, Boolean> predicateFunction = null;

  private static Function<Record, Boolean> createPredicateFunction(String key, ComparisonOperator comparisonOperator, Object compareValue) {
    if (compareValue instanceof Integer) {
      compareValue = ((Integer) compareValue).longValue();
    }
    Object finalCompareValue = compareValue;
    return record -> {
      Map<String, Object> map = record.getMapAttrNameToValueValue();
      if (!map.containsKey(key)) {
        return false;
      }
      Object value = map.get(key);
        if (value == null) {
            return false;
        }
      switch (comparisonOperator) {
        case EQUAL_TO:
          return value.equals(finalCompareValue);
        case GREATER_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) >= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN_OR_EQUAL_TO:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) < 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case GREATER_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) > 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        case LESS_THAN:
          if (value instanceof Comparable) {
            return ((Comparable) value).compareTo(finalCompareValue) <= 0;
          } else {
            throw new IllegalArgumentException("Value is not comparable");
          }
        default:
          throw new IllegalArgumentException("Invalid comparison operator");
      }
    };
  }

  private boolean isInitialized = false;
  private boolean committed = false;


  /****
   * Initialize the database and transaction, and table manager
   */
  private void init(String tableName) {
    db = FDBHelper.initialization();
    tx = FDBHelper.openTransaction(db);
    tableManager = new TableManagerImpl();
    recordTransformer = new RecordTransformer(tableName);
    recordAttributeStorePath = recordTransformer.getRecordAttributeStorePath();
    tableDirectory = FDBHelper.createOrOpenSubspace(tx, recordAttributeStorePath);
  }

  public Cursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Mode mode, boolean isUsingIndex) {
      this.tableName = tableName;
      this.operator = operator;
      this.mode = mode;
      this.isUsingIndex = isUsingIndex; // we don't need this for now
      this.predicateFunction = createPredicateFunction(attrName, operator, attrValue);
      init(tableName);

  }

  public Cursor(String tableName, Mode mode) {
    this.tableName = tableName;
    this.mode = mode;
    this.recordTransformer = new RecordTransformer(tableName);
    this.predicateFunction = null;
    init(tableName);

    TableMetadata tableMetadata = tableManager.getTableMetadata(tableName);
    if (tableMetadata == null) {
      this.iterator = null;
    }

  }

  public Cursor initializeCursor()
  {
    if (cursorStatus != CursorStatus.DIRECTION_SET) {
      System.out.println("Cursor not ready to be initialized or already initialized");
      return null;
    }

    TableMetadata tableMetadata = tableManager.getTableMetadata(tableName);

    List<String> primaryKeys = tableMetadata.getPrimaryKeys();

    Tuple pkExistPrefix = recordTransformer.getTableRecordExistTuplePrefix(primaryKeys);

    byte[] keyPrefixB = tableDirectory.pack(pkExistPrefix);

    Range range = Range.startsWith(keyPrefixB);
//    System.out.println("keyPrefixB " + Utils.byteArray2String(keyPrefixB));
//    System.out.println("tabledirectory " +Utils.byteArray2String(tableDirectory.pack()));

    assert (direction != Direction.UNSET);

    if (direction == Direction.FIRST_2_LAST){
      AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();
      this.iterator = iterator;
    }
    else if (direction == Direction.LAST_2_FIRST){
      AsyncIterator<KeyValue> iterator = tx.getRange(range,DBConf.MAX_RECORD,true).iterator();
      this.iterator = iterator;
    }

    cursorStatus = CursorStatus.ITERATOR_INITIALIZED;

    return this;
  }

  public Cursor moveToFirst() {
    if (direction != Direction.UNSET || cursorStatus != CursorStatus.UNINITIALIZED) {
      iterator = null;
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    direction = Direction.FIRST_2_LAST;
    cursorStatus = CursorStatus.DIRECTION_SET;
    return initializeCursor();
  }

  public Cursor moveToLast() {
    if (direction != Direction.UNSET || cursorStatus != CursorStatus.UNINITIALIZED) {
      this.iterator = null;
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    direction = Direction.LAST_2_FIRST;
    cursorStatus = CursorStatus.DIRECTION_SET;
    return initializeCursor();
  }

  public Record getNextRecord() {
    if (direction != Direction.FIRST_2_LAST) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (cursorStatus == CursorStatus.EOF){
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    currentPrimaryKeyValueTuple = null;
    currentRecord = null;
    return getCurrentRecord();

  }

  public Record getPreviousRecord() {
    if (direction != Direction.LAST_2_FIRST) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (cursorStatus == CursorStatus.EOF){
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    currentPrimaryKeyValueTuple = null;
    currentRecord = null;
    return getCurrentRecord();
  }


  public Record getCurrentRecord() {

    if (cursorStatus == CursorStatus.EOF){
      return null;
    }

    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      cursorStatus = CursorStatus.ERROR;
      return null;
    }

    if (currentRecord!= null) {
      return currentRecord;
    }

    Tuple recordExistTuple = null;
    try{
      if (iterator == null) {
        System.out.println("wtf iterator is null");
        cursorStatus = CursorStatus.ERROR;
        return null;
      }

      if (!iterator.hasNext()) {
//        System.out.println("iterator reach end, EOF!");
        cursorStatus = CursorStatus.EOF;
        return null;
      }

      KeyValue nextKeyValue = iterator.next();
      if (nextKeyValue == null) {
        System.out.println("wtf, get null from iterator.next()");
        cursorStatus = CursorStatus.ERROR;
        return null;
      }

//      System.out.println("Get key: " + Utils.byteArray2String(nextKeyValue.getKey()) + " value: " + Utils.byteArray2String( nextKeyValue.getValue()));
      recordExistTuple = Tuple.fromBytes(nextKeyValue.getKey());
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (recordExistTuple == null) {
      return null;
    }

    Tuple primaryKeyValueTuple = recordTransformer.getPrimaryKeyValueTuple(recordExistTuple);

    Record record = getRecordByPrimaryKeyValueTuple(primaryKeyValueTuple);

    // Apply predicate function
    while (predicateFunction != null && record != null && !predicateFunction.apply(record)){
//      System.out.println("predicate not satisfied, get next record");
      record = getCurrentRecord();
    }
    currentRecord = record;
    currentPrimaryKeyValueTuple = primaryKeyValueTuple;
    return record;

  }

  private Record getRecordByPrimaryKeyValueTuple(Tuple primaryKeyValueTuple) {
    TableMetadata tableMetadata = tableManager.getTableMetadata(tableName);
    Record currentRecord = new Record();
    for (String attributeName : tableMetadata.getAttributes().keySet()){
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        currentRecord.setAttrNameAndValue(attributeName, null);
      }
      else {
        Tuple attributeValueTuple = fdbkvPair.getValue();
        Object attributeValue = attributeValueTuple.get(0);
        currentRecord.setAttrNameAndValue(attributeName, attributeValue);
      }
    }
    return currentRecord;
  }

  public StatusCode dropRecord() {
    if(cursorStatus == CursorStatus.EOF){
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    if(currentPrimaryKeyValueTuple == null){
      return StatusCode.CURSOR_INVALID;
    }
    Tuple primaryKeyValueTuple = currentPrimaryKeyValueTuple;
    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    for (String attributeName : tableMetadata.getAttributes().keySet()){
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(primaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        continue;
      }
//      System.out.println(fdbkvPair);
//      System.out.println(fdbkvPair.getKey());
      FDBHelper.removeKeyValuePair(tx, tableDirectory, fdbkvPair.getKey());
      byte[] keyPrefixB = tableDirectory.pack(RecordTransformer.getTableRecordAttributeKeyTuplePrefix(attributeName));
      Range range = Range.startsWith(keyPrefixB);
      AsyncIterator<KeyValue> iterator = tx.getRange(range).iterator();
      if (!iterator.hasNext()){
        // the record we are deleating are the only record that have the attribute, so we need to shrink table metadata
        tableManager.dropAttributeTx(tx, tableName, attributeName);
      }
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode commit() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }
    if (cursorStatus == CursorStatus.EOF || cursorStatus == CursorStatus.ITERATOR_INITIALIZED ) {
      assert(FDBHelper.commitTransaction(tx));
      cursorStatus = CursorStatus.COMMITTED;
      return StatusCode.SUCCESS;
    }
    System.out.println("Cursor status is" + cursorStatus + " not ready to commit");
    return StatusCode.CURSOR_INVALID;
  }

  public StatusCode updateRecord(String[] attrNames, Object[] attrValues) {

    if (cursorStatus == CursorStatus.EOF){
      return StatusCode.CURSOR_REACH_TO_EOF;
    }
    if (cursorStatus != CursorStatus.ITERATOR_INITIALIZED) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    TableMetadata tableMetadata = tableManager.getTableMetadataTx(tx, tableName);
    Record record = currentRecord;

    for (int i = 0; i < attrNames.length; i++) {
      String attrName = attrNames[i];
      Object attrValue = attrValues[i];
      if (!tableMetadata.getAttributes().containsKey(attrName)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }
      record.setAttrNameAndValue(attrName, attrValue);
    }

    // IF attribute not in record, update mapMetadata
    boolean isAttributeTypeMatched = Arrays.stream(attrNames)
            .allMatch(attrName ->
                    tableMetadata.getAttributes().get(attrName) == null ||
                    record.getTypeForGivenAttrName(attrName)==tableMetadata.getAttributes().get(attrName));
    if (!isAttributeTypeMatched) {
      return StatusCode.DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED;
    }

    Map<String, Object> pkMap = record.getMapAttrNameToValueValue();

    Tuple primaryKeyValueTuple = Tuple.fromList(tableMetadata.getPrimaryKeys().stream().map(pkMap::get).collect(Collectors.toList()));
    Tuple primaryKeyValueTupleL = new Tuple().add(primaryKeyValueTuple);

    // Drop old record
    Tuple oldPrimaryKeyValueTuple = currentPrimaryKeyValueTuple;
    for (String attributeName : tableMetadata.getAttributes().keySet()){
      Tuple attributeKeyTuple = recordTransformer.getTableRecordAttributeKeyTuple(oldPrimaryKeyValueTuple, attributeName);
      FDBKVPair fdbkvPair = FDBHelper.getCertainKeyValuePairInSubdirectory(
              tableDirectory,
              tx,
              attributeKeyTuple,
              recordAttributeStorePath);
      if (fdbkvPair == null) {
        continue;
      }
//      System.out.println(fdbkvPair);
//      System.out.println(fdbkvPair.getKey());
      FDBHelper.removeKeyValuePair(tx, tableDirectory, fdbkvPair.getKey());
    }

    // Insert new record
    List<FDBKVPair> pairs = recordTransformer.convertToFDBKVPairs(record, primaryKeyValueTuple);
    for (FDBKVPair kvPair : pairs) {
      FDBHelper.setFDBKVPair(tableDirectory, tx, kvPair);
    }

    return StatusCode.SUCCESS;
  }

}
