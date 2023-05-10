/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.thssdb.rpc.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2023-05-10")
public class ExecuteStatementReq implements org.apache.thrift.TBase<ExecuteStatementReq, ExecuteStatementReq._Fields>, java.io.Serializable, Cloneable, Comparable<ExecuteStatementReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExecuteStatementReq");

  private static final org.apache.thrift.protocol.TField SESSION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField STATEMENT_FIELD_DESC = new org.apache.thrift.protocol.TField("statement", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ExecuteStatementReqStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ExecuteStatementReqTupleSchemeFactory();

  public long sessionId; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String statement; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SESSION_ID((short)1, "sessionId"),
    STATEMENT((short)2, "statement");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SESSION_ID
          return SESSION_ID;
        case 2: // STATEMENT
          return STATEMENT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __SESSIONID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SESSION_ID, new org.apache.thrift.meta_data.FieldMetaData("sessionId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.STATEMENT, new org.apache.thrift.meta_data.FieldMetaData("statement", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExecuteStatementReq.class, metaDataMap);
  }

  public ExecuteStatementReq() {
  }

  public ExecuteStatementReq(
    long sessionId,
    java.lang.String statement)
  {
    this();
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    this.statement = statement;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExecuteStatementReq(ExecuteStatementReq other) {
    __isset_bitfield = other.__isset_bitfield;
    this.sessionId = other.sessionId;
    if (other.isSetStatement()) {
      this.statement = other.statement;
    }
  }

  public ExecuteStatementReq deepCopy() {
    return new ExecuteStatementReq(this);
  }

  @Override
  public void clear() {
    setSessionIdIsSet(false);
    this.sessionId = 0;
    this.statement = null;
  }

  public long getSessionId() {
    return this.sessionId;
  }

  public ExecuteStatementReq setSessionId(long sessionId) {
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    return this;
  }

  public void unsetSessionId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  /** Returns true if field sessionId is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  public void setSessionIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SESSIONID_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getStatement() {
    return this.statement;
  }

  public ExecuteStatementReq setStatement(@org.apache.thrift.annotation.Nullable java.lang.String statement) {
    this.statement = statement;
    return this;
  }

  public void unsetStatement() {
    this.statement = null;
  }

  /** Returns true if field statement is set (has been assigned a value) and false otherwise */
  public boolean isSetStatement() {
    return this.statement != null;
  }

  public void setStatementIsSet(boolean value) {
    if (!value) {
      this.statement = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case SESSION_ID:
      if (value == null) {
        unsetSessionId();
      } else {
        setSessionId((java.lang.Long)value);
      }
      break;

    case STATEMENT:
      if (value == null) {
        unsetStatement();
      } else {
        setStatement((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SESSION_ID:
      return getSessionId();

    case STATEMENT:
      return getStatement();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case SESSION_ID:
      return isSetSessionId();
    case STATEMENT:
      return isSetStatement();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ExecuteStatementReq)
      return this.equals((ExecuteStatementReq)that);
    return false;
  }

  public boolean equals(ExecuteStatementReq that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_sessionId = true;
    boolean that_present_sessionId = true;
    if (this_present_sessionId || that_present_sessionId) {
      if (!(this_present_sessionId && that_present_sessionId))
        return false;
      if (this.sessionId != that.sessionId)
        return false;
    }

    boolean this_present_statement = true && this.isSetStatement();
    boolean that_present_statement = true && that.isSetStatement();
    if (this_present_statement || that_present_statement) {
      if (!(this_present_statement && that_present_statement))
        return false;
      if (!this.statement.equals(that.statement))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(sessionId);

    hashCode = hashCode * 8191 + ((isSetStatement()) ? 131071 : 524287);
    if (isSetStatement())
      hashCode = hashCode * 8191 + statement.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ExecuteStatementReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetSessionId()).compareTo(other.isSetSessionId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionId, other.sessionId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetStatement()).compareTo(other.isSetStatement());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatement()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.statement, other.statement);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ExecuteStatementReq(");
    boolean first = true;

    sb.append("sessionId:");
    sb.append(this.sessionId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("statement:");
    if (this.statement == null) {
      sb.append("null");
    } else {
      sb.append(this.statement);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'sessionId' because it's a primitive and you chose the non-beans generator.
    if (statement == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'statement' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExecuteStatementReqStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExecuteStatementReqStandardScheme getScheme() {
      return new ExecuteStatementReqStandardScheme();
    }
  }

  private static class ExecuteStatementReqStandardScheme extends org.apache.thrift.scheme.StandardScheme<ExecuteStatementReq> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExecuteStatementReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SESSION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.sessionId = iprot.readI64();
              struct.setSessionIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATEMENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.statement = iprot.readString();
              struct.setStatementIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetSessionId()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'sessionId' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExecuteStatementReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(SESSION_ID_FIELD_DESC);
      oprot.writeI64(struct.sessionId);
      oprot.writeFieldEnd();
      if (struct.statement != null) {
        oprot.writeFieldBegin(STATEMENT_FIELD_DESC);
        oprot.writeString(struct.statement);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExecuteStatementReqTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExecuteStatementReqTupleScheme getScheme() {
      return new ExecuteStatementReqTupleScheme();
    }
  }

  private static class ExecuteStatementReqTupleScheme extends org.apache.thrift.scheme.TupleScheme<ExecuteStatementReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExecuteStatementReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI64(struct.sessionId);
      oprot.writeString(struct.statement);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExecuteStatementReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.sessionId = iprot.readI64();
      struct.setSessionIdIsSet(true);
      struct.statement = iprot.readString();
      struct.setStatementIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

