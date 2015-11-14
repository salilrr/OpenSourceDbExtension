/*
 * 
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Salil Rajadhyaksha
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Matcher;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;


/**
 * Implementation of the SSN data type.
 */
public class ValueSSN extends Value {

    private static final ValueSSN EMPTY = new ValueSSN("000000000");

    /**
     * The SSN data.
     */
    protected final String value;

    protected ValueSSN(String value)  {
    	
        this.value = value;
        
    }

    @Override
    public String getSQL()  {
    	
    		
    		return StringUtils.quoteStringSQL(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueSSN
                && value.equals(((ValueSSN) other).value);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueSSN v = (ValueSSN) o;
        return mode.compareString(value, v.value, false);
    }

    @Override
    public String getString() {
    	return value;
    }

    @Override
    public long getPrecision() {
        return 9;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setString(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return value.length();
    }

    @Override
    public int getMemory() {
        return value.length() * 2 + 48;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return getNew(value.substring(0, p));
    }

    @Override
    public int hashCode() {
       return value.hashCode();
    }

    @Override
    public int getType() {
        return Value.STRING;
    }

    /**
     * Get or create a string value for the given SSN.
     *
     * @param s the string
     * @return the value
     */
    public static Value get(String s) {
    	   try {
               if(s.length()!=9)//check the length
            	   throw new DbException(new SQLException("Invalid SSN value.The number of characters should be 9"));
           } catch (DbException ex) {
               throw DbException.convert(ex);
           }
    	
    	  
    	  java.util.regex.Pattern ssnPattern=java.util.regex.Pattern.compile("[0-9]+");//check the characters.
    	  Matcher ssnMatcher=ssnPattern.matcher(s);
    	  boolean isSSNValid=ssnMatcher.matches();
    	  try {
              if(!isSSNValid)
           	   throw new DbException(new SQLException("Invalid SSN value.SSN cannot contain any characters other than digits"));
          } catch (DbException ex) {
              throw DbException.convert(ex);
          }
    	  String ssn = s.substring(0, 3) + "-" +s.substring(3, 5) + "-"+s.substring(5, s.length());
    	return get(ssn,false);
    }

    /**
     * Get or create a string value for the given SSN.
     *
     * @param s-SSN
     * @param treatEmptyStringsAsNull whether or not to treat empty strings as
     *            NULL
     * @return the value
     */
    public static Value get(String s, boolean treatEmptyStringsAsNull) {
        if (s.isEmpty()) {
            return treatEmptyStringsAsNull ? ValueNull.INSTANCE : EMPTY;
        }
      ValueSSN obj = new ValueSSN(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return Value.cache(obj);
    }

    /**
     * Create a new String value of the current class.
     * This method is meant to be overridden by subclasses.
     *
     * @param s the string
     * @return the value
     */
    protected Value getNew(String s) {
        return ValueSSN.get(s);
    }

}
