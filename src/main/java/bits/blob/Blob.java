/*
 * Copyright (c) 2014. Massachusetts Institute of Technology
 * Released under the BSD 2-Clause License
 * http://opensource.org/licenses/BSD-2-Clause
 */
package bits.blob;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;
import java.net.*;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;


/**
 * Blob is a flexible container object that stores data as a tree. Parent nodes
 * may consist of {@link java.util.Map}s, {@link java.util.List}s,
 * {@link java.util.Set}s and other {@code Blob}s. Leaf nodes may be any
 * other arbitrary {@link java.lang.Object} or {@code null} value, with special
 * support for {@link java.lang.Number}s and {@link java.lang.String}s.
 *
 * <p> The core of Blob are the operations for {@link #get}, {@link #put},
 * {@link #add}, {@link #remove}, {@link #containsKey}, {@link #containsValue},
 * and {@link #size}, which all accept a key sequence as a vararge argument and
 * provide a convenient way for accessing deeply nested structures.
 * Blob also has a {@link #slice} operation that will produce a subtree view of
 * the blob.
 *
 * <p> Most operations require, as a vararg, an array of key objects. Each key is
 * used to navigate down one level of the Blob. For example:
 *
 * <p>{@code Object obj = blob.get(1, "the", "cat");}</p>
 *
 * <p>Here, there are three keys. The first key is an integer, and thus it may
 * return a result whether the first level is a List or a Map or a Blob. The
 * next two keys are Strings. While Strings and other objects may be used as
 * keys in Maps, they are invalid keys for Lists. Thus, if the blob.get(1)
 * contains a List, the key is unusable and get(1, "the") will return null. Sets
 * do not contain keys in either form and elements must be retrieved via
 * iteration.
 *
 * <p> Blob is not thread-safe, and currently does not protect against concurrent
 * modifications on various views.
 *
 * @author Philip DeCamp
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class Blob {

    /** Unique object used internally to indicate ADD operation. */
    private static final Object ADD_KEY = new Object();

    /**
     * Specifies max number of NULL items to add to a list before converting it to a Map to conserve memory.
     * IE, don't create a list with 1,000,000,000 entries just to hold the key "Integer( 1000000000 )".
     */
    private static final int MAX_LIST_PADDING = 100;

    private final Blob mParent;
    private final Object[] mPosition;

    private Object mRoot; // Cannot be Blob object.


    /**
     * Creates a new Blob that contains no data.
     */
    public Blob() {
        mRoot = null;
        mParent = null;
        mPosition = null;
    }

    /**
     * Creates a new Blob with the specified root data. The root data of the
     * Blob does not need to be a container; it may be any object or even null.
     * If the root is not a container, it will not be possible to add additional
     * data items to the Blob without losing this piece of data.
     *
     * @param root  Data to be held by the Blob.
     */
    public Blob( Object root ) {
        mRoot = deblob( root );
        mParent = null;
        mPosition = null;
    }


    Blob( Blob parent, Object[] position, Object root ) {
        mRoot = root;
        mParent = parent;
        mPosition = position.clone();
    }



    // **************************************************************
    // Core Operations
    // **************************************************************

    /**
     * Checks if the key is present in the Blob.
     *
     * <p>Example:
     *
     * <p><code>
     * Blob b = new Blob(); <br>
     * b.put(3, "cat", "value"); <br>
     * b.containsKey(3); <br>
     * //returns true <br>
     * b.containsKey(3, "cat"); <br>
     * //returns true<br>
     * b.containsKey(3, "cat", "value"); <br>
     * //returns false.
     * </code>
     *
     * @param keys  Sequence of keys.
     * @return true if all keys are contained in the Blob.
     */
    public boolean containsKey( Object... keys ) {
        if( keys.length == 0 ) {
            return get( null, 0, 0 ) != null;
        }
        Object data = get( keys, 0, keys.length - 1 );
        return objectContainsKey( data, keys[keys.length - 1] );
    }

    /**
     * Checks if a value is present in the Blob at a location specified by an
     * arbitrary list of keys.
     *
     * <p>Example:
     *
     * <p><code>
     * Blob b = new Blob(); <br>
     * b.put(3, "cat", "value"); <br>
     * b.containsValue(3); <br>
     * //returns false <br>
     * b.containsValue(3, "cat"); <br>
     * //returns false<br>
     * b.containsValue(3, "value"); <br>
     * //returns true.
     * </code>
     *
     * @param keysAndValue Arbitrary series of keys followed by a single value.
     * @return true if value is contained as specified level.
     */
    public boolean containsValue( Object... keysAndValue ) {
        if( keysAndValue == null || keysAndValue.length == 0 ) {
            return false;
        }
        Object data = get( keysAndValue, 0, keysAndValue.length - 1 );
        return objectContainsValue( data, keysAndValue[keysAndValue.length - 1] );
    }

    /**
     * Returns an object from the Blob.
     *
     * <p>Example:
     *
     * <p><code>
     * Blob b = new Blob(); <br>
     * b.put(3, "cat", "value"); <br>
     * b.get(); <br>
     * //returns [ null, null, null, { "cat" => "value" } ] <br>
     * b.get(3); <br>
     * //returns { "cat" => "value" } <br>
     * b.get(3, "cat"); <br>
     * //returns "value"
     * </code>
     *
     * @param keys  Key sequence
     * @return Object associated with provided keys, or {@code null} if none exists.
     */
    public Object get( Object... keys ) {
        return get( keys, 0, keys.length );
    }

    /**
     * Retrieves and casts a single object from the Blob if the object exists
     * and belongs to the specified class.
     *
     * @param clazz  Class type for the object to retrieve.
     * @param keys   Keys for object.
     * @return The object associated with the keys if that object exists and
     *         belongs to the specified class. Otherwise, null.
     */
    public <S> S getType( Class<S> clazz, Object... keys ) {
        Object data = get( keys, 0, keys.length );
        return clazz.isInstance( data ) ? (S)data : null;
    }

    /**
     * @return Associated object only if a {@link java.util.Map} object.
     */
    public Map<?,?> getMap( Object... keys ) {
        Object item = get( keys );
        return (item instanceof Map) ? (Map<?,?>)item : null;
    }

    /**
     * @return A value only if a {@link java.util.List} object.
     */
    public List<?> getList( Object... keys ) {
        Object item = get( keys );
        return (item instanceof List) ? (List<?>)item : null;
    }

    /**
     * @return A value only if a {@link java.util.Set} object.
     */
    public Set<?> getSet( Object... keys ) {
        Object item = get( keys );
        return (item instanceof Set) ? (Set<?>)item : null;
    }

    /**
     * @return A value only if it is a {@link java.lang.String} object.
     */
    public String getString( Object... keys ) {
        Object item = get( keys );
        return (item instanceof String) ? (String)item : null;
    }

    /**
     * Makes a best effort to interpret a value as a boolean.
     * <p>
     * True values are true booleans, any {@link java.lang.Number} that does not equal zero, and any of the Strings
     * "y", "yes", "t" or "true" with any case.
     * <p>
     * False values are False booleans, any {@link java.lang.Number} that equals zero, and any of the strings
     * "n", "no", "f" or "false" with any case.
     * <p>
     * Other values are considered undefined as booleans and a {@code null} will be returned.
     *
     * @return true if keys are associated with a value interpretable as true, false if a value intepretabel as false,
     *         otherwise {@code null}.
     */
    public Boolean getBoolean( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Boolean ) {
            return (Boolean)item;
        }

        if( item == null ) {
            return null;
        }

        if( item instanceof String ) {
            String s = (String)item;
            int len = s.length();
            if( len == 0 ) {
                return Boolean.FALSE;
            }

            switch( s.charAt( 0 ) ) {
            case 'y':
            case 'Y':
                if( len == 1 || len == 3 && s.equalsIgnoreCase( "yes" ) ) {
                    return Boolean.TRUE;
                }
                break;
            case 't':
            case 'T':
                if( len == 1 || len == 4 && s.equalsIgnoreCase( "true" ) ) {
                    return Boolean.TRUE;
                }
                break;
            case 'n':
            case 'N':
                if( len == 1 || len == 2 && s.equalsIgnoreCase( "no" ) ) {
                    return Boolean.FALSE;
                }
                break;
            case 'f':
            case 'F':
                if( len == 1 || len == 5 && s.equalsIgnoreCase( "false" ) ) {
                    return Boolean.FALSE;
                }
                break;
            }
            return null;
        }

        if( item instanceof Number ) {
            return ((Number)item).doubleValue() != 0;
        }

        return null;
    }

    /**
     * @return If value is an instance of {@link java.lang.Number}, that value is converted to an Integer and returned.
     *         Otherwise {@code null}.
     */
    public Integer getInt( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Integer ) {
            return (Integer)item;
        }
        if( item instanceof Number ) {
            return ((Number)item).intValue();
        }

        return null;
    }

    /**
     * @return If value is an instance of {@link java.lang.Number}, that value is converted to a Long and returned.
     *         Otherwise {@code null}.
     */
    public Long getLong( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Long ) {
            return (Long)item;
        }
        if( item instanceof Number ) {
            return new Long( ((Number)item).longValue() );
        }

        return null;
    }

    /**
     * @return If value is an instance of {@link java.lang.Number}, that value is converted to a Float and returned.
     *         Otherwise {@code null}.
     */
    public Float getFloat( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Float ) {
            return (Float)item;
        }
        if( item instanceof Number ) {
            return new Float( ((Number)item).floatValue() );
        }

        return null;
    }

    /**
     * @return If value is an instance of {@link java.lang.Number}, that value is converted to a Double and returned.
     *         Otherwise {@code null}.
     */
    public Double getDouble( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Double ) {
            return (Double)item;
        }
        if( item instanceof Number ) {
            return new Double( ((Number)item).doubleValue() );
        }

        return null;
    }


    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if not-null, otherwise {@code defaultVal}.
     * @see #get
     */
    public Object tryGet( Object defaultVal, Object... keys ) {
        Object item = get( keys );
        return item != null ? item : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is a String, otherwise {@code defaultVal}.
     * @see #getString
     */
    public String tryGetString( String defaultVal, Object... keys ) {
        Object item = get( keys );
        return ( item instanceof String ) ? (String)item : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is interpretable as a Boolean,
     *         otherwise {@code defaultVal}.
     *
     * @see #getBoolean
     */
    public boolean tryGetBoolean( boolean defaultVal, Object... keys ) {
        Boolean b = getBoolean( keys );
        return b != null ? b : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is a Number, otherwise {@code defaultVal}.
     * @see #getInt
     */
    public int tryGetInt( int defaultVal, Object... keys ) {
        Integer ret = getInt( keys );
        return ret != null ? ret : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is a Number, otherwise {@code defaultVal}.
     * @see #getLong
     */
    public long tryGetLong( long defaultVal, Object... keys ) {
        Long ret = getLong( keys );
        return ret != null ? ret : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is a Number, otherwise {@code defaultVal}.
     * @see #getFloat
     */
    public float tryGetFloat( float defaultVal, Object... keys ) {
        Float ret = getFloat( keys );
        return ret != null ? ret : defaultVal;
    }

    /**
     * @param defaultVal A default value to return if no value is found.
     * @param keys       Key sequence.
     * @return Value associated with {@code keys} if exists and is a Number, otherwise {@code defaultVal}.
     * @see #getDouble
     */
    public double tryGetDouble( double defaultVal, Object... keys ) {
        Double ret = getDouble( keys );
        return ret != null ? ret : defaultVal;
    }

    /**
     * Like {@code #get}, but throws an IOException if object is null.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or null.
     * @see #get
     */
    public Object forceGet( Object... keys ) throws IOException {
        Object obj = get( keys );
        if( obj != null ) {
            return obj;
        }
        throw new IOException( formatKeys( true, keys ) + " not defined.");
    }

    /**
     * Like {@code #getBoolean}, but throws an IOException if boolean cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not interpretable as a boolean.
     * @see #getBoolean
     */
    public boolean forceGetBoolean( Object... keys ) throws IOException {
        Boolean ret = getBoolean( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys )  + " not defined or not interpretable as a boolean.");
    }

    /**
     * Like {@code #getInt}, but throws an IOException if an int cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not interpretable as an integer.
     * @see #getInt
     */
    public int forceGetInt( Object... keys ) throws IOException {
        Integer ret = getInt( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys )  + " not defined or not interpretable as an integer.");
    }

    /**
     * Like {@code #getLong}, but throws an IOException if a long cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not interpretable as a long.
     * @see #getLong
     */
    public long forceGetLong( Object... keys ) throws IOException {
        Long ret = getLong( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys )  + " not defined or not interpretable as a long integer.");
    }

    /**
     * Like {@code #getFloat}, but throws an IOException if a float cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not interpretable as a float.
     * @see #getFloat
     */
    public float forceGetFloat( Object... keys ) throws IOException {
        Float ret = getFloat( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys ) + " not defined or not interpretable as a float.");
    }

    /**
     * Like {@code #getDouble}, but throws an IOException if a double cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not interpretable as a double.
     * @see #getFloat
     */
    public double forceGetDouble( Object... keys ) throws IOException {
        Double ret = getDouble( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys ) + " not defined or not interpretable as a double.");
    }

    /**
     * Like {@code #getDouble}, but throws an IOException if a String cannot be returned.
     * <p>
     * The purpose of the {@code force} methods is to make it easier to throw
     * readable parse exceptions when required, rather than force the user to
     * check the return results after each query.
     *
     * @param keys key sequence
     * @return Value associated with {@code keys}.
     * @throws IOException if value is undefined or not a string.
     * @see #getString
     */
    public String forceGetString( Object... keys ) throws IOException {
        String ret = getString( keys );
        if( ret != null ) {
            return ret;
        }
        throw new IOException( formatKeys( true, keys ) + " not defined or not a string.");
    }



    /**
     * This operation is identicalto put(), except that the last key is implied.
     *
     * <p>If the keys specify a {@code Map}, the last key will be the key
     * and the value. (Key is mapped to itself).
     *
     * <p>If the keys specify a {@code List}, the value will be added to the
     * list. This means there is an implied key: the integer size of the List.
     *
     * <p>If the keys specify a {@code Set}, the value is simply added.
     *
     * @param keysAndValue Arbitrary sequence of keys followed by a value.
     * @return true iff Blob is modified as a result.
     */
    public boolean add( Object... keysAndValue ) {
        if( keysAndValue.length == 0 ) {
            return false;
        }
        return add( keysAndValue, 0, keysAndValue.length - 1, deblob( keysAndValue[keysAndValue.length - 1] ) );
    }

    /**
     * Puts object into a container at the specified key sequence in the Blob.
     * If no such container/key sequence exists in the Blob, it will be created,
     * overwriting existing data in the Blob as necessary. Keys that are
     * {@code Integers} may correspond to {@code Lists} or {@code Maps}, while
     * other keys will be made to correspond to {@code Maps} only.
     *
     * <p>Attempting to insert keys into a list where the key is:
     * <ul>
     * <li>Not an integer</li>
     * <li>A negative integer</li>
     * <li>An integer {@code n} where {@code n + 100 > list.size()}</li>
     * </ul>
     * will cause the list to be converted to a Map, where every item that was
     * in the List will be placed in the new Map using its list index as an
     * Integer key.
     *
     * <p>Attempting to insert any keys into a Set object will cause that Set to be
     * converted to a Map, where every item that was in the Set will be placed
     * in the new Map using itself as a key.
     *
     * @param keysAndValue Arbitrary sequence of keys followed by a value.
     * @return value that was previously located at specified key sequence
     */
    public Object put( Object... keysAndValue ) {
        if( keysAndValue.length == 0 ) {
            return null;
        }
        return put( keysAndValue, 0, keysAndValue.length - 1, deblob( keysAndValue[keysAndValue.length - 1] ) );
    }

    /**
     * Removes object stored at specified key sequence. This may be used to
     * remove entire subtrees from the Blob.
     *
     * @return value that was removed, if any.
     */
    public Object remove( Object... keys ) {
        return remove( keys, 0, keys.length );
    }


    public void clear() {
        clearRoot();
    }


    public int size( Object... keys ) {
        return objectSize( get( keys ) );
    }


    // **************************************************************
    // View Methods
    // **************************************************************

    /**
     * Creates a view Blob from data in this Blob. Any changes that occur to the
     * view apply to this blob. Concurrent modifications (modifying the slice
     * after modifying this blob) has undefined results, and may throw a
     * ConcurrentModificationException.
     *
     * <p>Examples:
     *
     * <p><code>
     * Blob blob = new Blob();  <br/>
     * blob.put("a", "b", 3);  <br/>
     * Blob slice = blob.slice("a");  <br/>
     * slice.get("b");  <br/>
     * \\returns 3  <br/>
     * slice.put("b", 0);  <br/>
     * slice.get("b");  <br/>
     * \\returns 0  <br/>
     * blob.get("a", "b"); <br/>
     * \\returns 0  <br/>
     * blob.put("a", "c");  <br/>
     * slice.get("b"); <br/>
     * \\undefined result<br/>
     * </code>
     *
     * @return a Blob that acts as a view of a subset of this Blob.
     */
    public Blob slice( Object... keys ) {
        Object item = get( keys );
        return new Blob( this, keys, item );
    }

    /**
     * If this blob is a slice view, this method can be used to get the
     * slice's path. While use of this method is discouraged as it
     * breaks the slice abstraction, it can be useful when you really
     * need the absolute path to a value, particularly when constructing
     * error messages.
     *
     * @return keys used to select this slice.
     */
    public Object[] sliceKeys() {
        return mPosition == null ? new Object[0] : mPosition.clone();
    }

    /**
     * If this blob is a slice view, this method can be used to get the
     * parent blob. Use of this method is discouraged.
     */
    public Blob sliceParent() {
        return mParent;
    }

    /**
     * Provides a view of a Set of keys. This is only valid if the object
     * associated with {@code keys} is a Map or List (Lists are treated
     * as Maps with keys that are sequential Integers).
     *
     * @param keys Key sequence
     * @return View of associated collection, or {@code null}.
     */
    public Set<?> keySet( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Map ) {
            return ((Map<?, ?>)item).keySet();
        } else if( item instanceof List ) {
            return new ListIndexSet( (List<?>)item );
        }
        return Collections.emptySet();
    }

    /**
     * Provides a view of a Set of entries. This is only valid if the object
     * associated with {@code keys} is a Map or List (Lists are treated as Maps with
     * keys that are sequential Integers).
     *
     * @param keys Key sequence
     * @return View of associated collection, or {@code null}.
     */
    public Set<Map.Entry<?, ?>> entrySet( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Map ) {
            return ((Map)item).entrySet();

        } else if( item instanceof List ) {
            return new ListEntrySet( (List<?>)item );
        }
        return Collections.emptySet();
    }

    /**
     * Provides a view of a Collection of values. This is only valid if the object
     * associated with {@code keys} is a Map, List or Set.
     *
     * @param keys Key sequence
     * @return View of associated collection, or {@code null}.
     */
    public Collection<?> values( Object... keys ) {
        Object item = get( keys );
        if( item instanceof Map ) {
            return ((Map)item).values();

        } else if( item instanceof List ) {
            return (List)item;

        } else if( item instanceof Set ) {
            return (Set)item;
        }
        return Collections.emptySet();
    }



    // **************************************************************
    // IO + Conversions
    // **************************************************************


    public String toString() {
        StringBuilder b = new StringBuilder();
        format( "", b );
        return b.toString();
    }


    public void format( String indent, StringBuilder out ) {
        if( indent == null ) {
            indent = "";
        }
        out.append( indent );
        out.append( "Blob: " );
        print( mRoot, out, indent + "  " );
    }


    public String formatKeys( boolean absolute, Object... keys ) {
        StringBuilder s = new StringBuilder();
        doFormatKeys( absolute, keys, s );
        return s.toString();
    }


    public String toYaml() {
        return new Yaml().dump( mRoot );
    }


    public void writeYaml( File outputFile ) throws IOException {
        Writer out = new BufferedWriter( new FileWriter( outputFile ) );
        writeYaml( new FileWriter( outputFile ) );
        out.close();
    }


    public void writeYaml( Writer writer ) throws IOException {
        new Yaml().dump( mRoot, writer );
    }


    public static Blob readYaml( File inputFile ) throws IOException {
        Reader reader = new FileReader( inputFile );
        try {
            return readYaml( reader );
        } finally {
            try {
                reader.close();
            } catch( IOException exc ) {}
        }
    }


    public static Blob readYaml( URL url ) throws IOException {
        URLConnection conn = null;
        InputStream in = null;

        try {
            conn = url.openConnection();
            in = conn.getInputStream();
            return readYaml( new BufferedReader( new InputStreamReader( in ) ) );
        } finally {
            try {
                if( in != null ) {
                    in.close();
                }
            } catch( Exception ee ) {}
        }
    }


    public static Blob readYaml( Reader reader ) throws IOException {
        Iterator<Object> iter = new Yaml().loadAll( reader ).iterator();
        if( !iter.hasNext() ) {
            return null;
        }

        return new Blob( iter.next() );
    }


    public static Blob readYaml( String yamlText ) throws IOException {
        return new Blob( new Yaml().load( yamlText ) );
    }



    // **************************************************************
    // Basic method implementations.
    // **************************************************************


    private Object get( Object[] keys, int offset, int length ) {
        Object data = mRoot;

        for( int i = offset; i < offset + length && data != null; i++ ) {
            Object key = keys[i];

            if( data instanceof Map ) {
                data = ((Map<?, ?>)data).get( key );

            } else if( data instanceof List ) {
                if( !(key instanceof Integer) )
                    return null;

                int index = (Integer)key;
                if( index < 0 || index >= ((List)data).size() )
                    return null;

                data = ((List)data).get( index );

            } else if( data instanceof Blob ) {
                data = ((Blob)data).mRoot;
                i--;

            } else {
                return null;
            }
        }

        return data;
    }


    private boolean add( Object[] keys, int offset, int length, Object value ) {
        Object data = objectMakePath( this, keys, offset, length, true );

        if( data instanceof Map ) {
            Object ret = ((Map<Object, Object>)data).put( value, value );
            return value != ret || value != null && !value.equals( ret );

        } else if( data instanceof List ) {
            return ((List<Object>)data).add( value );

        } else if( data instanceof Set ) {
            return ((Set<Object>)data).add( value );

        } else {
            throw new InternalError( "Invalid case in add(): " + getClass().getName() );
        }
    }


    private Object put( Object[] keys, int offset, int length, Object value ) {
        Object container = objectMakePath( this, keys, offset, length, false );

        if( container instanceof Map ) {
            return ((Map)container).put( keys[offset + length - 1], value );

        } else if( container instanceof List ) {
            List list = (List)container;
            int idx = (Integer)keys[offset + length - 1];

            if( idx < list.size() ) {
                Object ret = list.get( idx );
                list.set( idx, value );
                return ret;
            }

            while( idx > list.size() )
                list.add( null );

            list.add( value );
            return null;

        } else if( container instanceof Blob ) {
            Blob blob = (Blob)container;
            Object ret = blob.mRoot;
            blob.setRoot( value );

            return ret;

        } else {
            throw new InternalError( "Invalid case in put(): " + getClass().getName() );
        }

    }


    private Object remove( Object[] keys, int offset, int length ) {
        if( length == 0 ) {
            Object o = mRoot;
            clearRoot();
            return o;
        }

        Object data = get( keys, offset, length - 1 );
        if( data == null )
            return null;

        if( data instanceof Map ) {
            Map<Object, Object> map = (Map<Object, Object>)data;
            return map.remove( keys[offset + length - 1] );
        }

        if( data instanceof List ) {
            List<Object> list = (List<Object>)data;

            if( !(keys[offset + length - 1] instanceof Integer) ) {
                return null;
            }

            int index = (Integer)keys[keys.length - 1];
            if( index >= 0 && index < list.size() )
                return list.remove( index );

            return null;
        }

        if( data instanceof Set ) {
            Set<Object> set = (Set<Object>)data;
            return set.remove( keys[keys.length - 1] );
        }

        return null;
    }




    private void print( Object data, StringBuilder buf, String indent ) {
        if( data == null ) {
            buf.append( "null\n" );

        } else if( data instanceof Map ) {
            Map<Object, Object> map = (Map<Object, Object>)data;

            if( map.size() == 0 ) {
                buf.append( "{}\n" );
                return;
            }

            buf.append( '\n' );

            for( Map.Entry<Object, Object> entry : map.entrySet() ) {
                buf.append( indent );
                buf.append( entry.getKey() );
                buf.append( ": " );
                print( entry.getValue(), buf, indent + "  " );
            }

        } else if( data instanceof List ) {
            List<Object> list = (List<Object>)data;

            if( list.size() == 0 ) {
                buf.append( "[]\n" );
                return;
            }

            buf.append( '\n' );

            for( int i = 0; i < list.size(); i++ ) {
                buf.append( indent );
                buf.append( "- " );
                print( list.get( i ), buf, indent + "  " );
            }

        } else if( data instanceof Set ) {
            Set<Object> set = (Set<Object>)data;

            if( set.size() == 0 ) {
                buf.append( "<>\n" );
                return;
            }

            buf.append( '\n' );

            for( Object obj : set ) {
                buf.append( indent );
                buf.append( "? " );
                print( obj, buf, indent + "  " );
            }

        } else if( data instanceof Blob ) {
            buf.append( '\n' );
            ((Blob)data).format( indent + "  ", buf );

        } else {
            buf.append( data.toString() );
            buf.append( '\n' );
        }
    }


    private void setRoot( Object root ) {
        if( mRoot == root )
            return;

        mRoot = root;
        if( mParent != null ) {
            mParent.put( mPosition, 0, mPosition.length, root );
        }
    }


    private void doFormatKeys( boolean absolute, Object[] keys, StringBuilder out ) {
        if( absolute ) {
            if( mParent != null ) {
                mParent.doFormatKeys( true, mPosition, out );
            }
        }

        if( keys == null ) {
            return;
        }

        for( Object k: keys ) {
            if( out.length() != 0 ) {
                out.append( ':' );
            }
            out.append( k == null ? "<null>" : k.toString() );
        }
    }


    private void clearRoot() {
        if( mRoot == null )
            return;

        mRoot = null;
        if( mParent != null )
            mParent.remove( mPosition, 0, mPosition.length );
    }


    private static final Pattern JSON_ESCAPE_PAT = Pattern.compile( "[\"\b\f\n\r\t]", Pattern.MULTILINE );


    // **************************************************************
    // Low-level operations.
    // **************************************************************


    private static Object deblob( Object obj ) {
        while( obj instanceof Blob )
            obj = ((Blob)obj).mRoot;

        return obj;
    }


    private static boolean keyFits( Object key, Object data ) {
        if( data == null ) {
            return false;
        }
        if( data instanceof Map ) {
            return true;
        }

        if( data instanceof List ) {
            if( key == ADD_KEY ) {
                return true;
            }
            if( !(key instanceof Integer) ) {
                return false;
            }
            int n = (Integer)key;
            return n >= 0 && n - MAX_LIST_PADDING < ((List)data).size();
        }

        if( data instanceof Set ) {
            return key == ADD_KEY;
        }

        if( data instanceof Blob ) {
            return true;
        }

        return false;
    }


    private static Object newContainerForKey( Object key, Object prevContainer ) {
        if( key == ADD_KEY )
            return new ArrayList<Object>();

        if( key instanceof Integer ) {
            int n = (Integer)key;
            if( n >= 0 && n < MAX_LIST_PADDING ) {
                return new ArrayList<Object>();
            }
        }

        LinkedHashMap<Object, Object> ret = new LinkedHashMap<Object, Object>();

        if( prevContainer != null ) {
            if( prevContainer instanceof List ) {
                List p = (List)prevContainer;
                for( int i = 0; i < p.size(); i++ ) {
                    ret.put( i, p.get( i ) );
                }
            } else if( prevContainer instanceof Set ) {
                Set p = (Set)prevContainer;
                for( Object pp : p ) {
                    ret.put( pp, pp );
                }
            }
        }

        return ret;
    }


    private static int objectSize( Object data ) {
        if( data == null ) {
            return 0;

        } else if( data instanceof Map ) {
            return ((Map)data).size();

        } else if( data instanceof List ) {
            return ((List)data).size();

        } else if( data instanceof Set ) {
            return ((Set)data).size();

        } else {
            return 0;
        }
    }


    private static boolean objectContainsKey( Object data, Object key ) {
        if( data == null ) {
            return false;

        } else if( data instanceof Map ) {
            return ((Map<?, ?>)data).containsKey( key );

        } else if( data instanceof List ) {
            if( !(key instanceof Integer) )
                return false;

            int index = (Integer)key;
            return index >= 0 && index < ((List<?>)data).size();

        } else if( data instanceof Set ) {
            return ((Set)data).contains( key );

        } else {
            return false;
        }
    }


    private static boolean objectContainsValue( Object data, Object value ) {
        if( data == null ) {
            return false;

        } else if( data instanceof Map ) {
            return ((Map)data).containsValue( value );

        } else if( data instanceof List ) {
            return ((List)data).contains( value );

        } else if( data instanceof Set ) {
            return ((Set)data).contains( value );

        } else {
            return false;
        }
    }


    private static Object objectMakePath( Object data, Object[] keys, int offset, int length, boolean addLastKey ) {
        Object node = data;
        Object key;

        if( length == 0 ) {
            if( addLastKey ) {
                key = ADD_KEY;
            } else if( node instanceof Blob ) {
                return node;
            } else {
                return null;
            }
        } else {
            key = keys[offset];
        }

        if( node instanceof Blob ) {
            Blob blob = (Blob)node;
            if( !keyFits( key, blob.mRoot ) )
                blob.setRoot( newContainerForKey( key, blob.mRoot ) );

            node = blob.mRoot;

        } else if( !keyFits( key, node ) ) {
            return null;
        }

        final int stop = offset + length + (addLastKey ? 1 : 0);

        for( int i = offset + 1; i < stop; i++ ) {
            Object parent = node;
            Object parentKey = key;

            if( addLastKey && i == stop - 1 ) {
                key = ADD_KEY;
            } else {
                key = keys[i];
            }

            if( parent instanceof Map ) {
                Map parentMap = (Map)parent;
                node = deblob( parentMap.get( parentKey ) );

                if( !keyFits( key, node ) ) {
                    node = newContainerForKey( key, node );
                    parentMap.put( parentKey, node );
                }

            } else {
                List parentList = (List)parent;
                int idx = (Integer)parentKey;

                if( idx >= 0 && idx < parentList.size() ) {
                    node = deblob( parentList.get( idx ) );

                    if( !keyFits( key, node ) ) {
                        node = newContainerForKey( key, node );
                        parentList.set( idx, node );
                    }

                } else {
                    while( parentList.size() < idx )
                        parentList.add( null );

                    node = newContainerForKey( key, null );
                    parentList.add( node );
                }
            }
        }

        return node;
    }



    // **************************************************************
    // View implementations
    // **************************************************************


    static final class SimpleEntry<K, V> implements Map.Entry<K, V> {

        private final K mKey;
        private final V mValue;

        SimpleEntry( K key, V value ) {
            mKey = key;
            mValue = value;
        }


        public K getKey() {
            return mKey;
        }

        public V getValue() {
            return mValue;
        }

        public V setValue( V value ) {
            throw new UnsupportedOperationException();
        }


        public int hashCode() {
            return (mKey == null ? 0 : mKey.hashCode()) ^
                   (mValue == null ? 0 : mValue.hashCode());
        }

        public final boolean equals( Object obj ) {
            if( !(obj instanceof Map.Entry) )
                return false;

            Object key = ((Map.Entry)obj).getKey();
            if( mKey == key || mKey != null && mKey.equals( key ) ) {
                Object v1 = mValue;
                Object v2 = ((Map.Entry)obj).getValue();

                return (v1 == v2 || v1 != null && v1.equals( v2 ));
            }

            return false;
        }

        public final String toString() {
            return mKey + "=" + mValue;
        }

    }


    static Iterator sEmptyIterator = new Iterator() {
        public boolean hasNext() {
            return false;
        }

        public Object next() {
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new IllegalStateException();
        }
    };


    static final class ToIndexIterator implements Iterator<Integer> {

        private final Iterator<?> mIter;
        private int mIndex = 0;


        ToIndexIterator( Iterator<?> iter ) {
            mIter = iter;
        }


        public boolean hasNext() {
            return mIter.hasNext();
        }

        public Integer next() {
            mIter.next();
            return mIndex++;
        }

        public void remove() {
            mIter.remove();
            mIndex--;
        }

    }


    static final class ToIndexEntryIterator implements Iterator<Map.Entry<?, ?>> {

        private final Iterator mIter;
        private int mIndex = 0;


        ToIndexEntryIterator( Iterator iter ) {
            mIter = iter;
        }


        public boolean hasNext() {
            return mIter.hasNext();
        }

        public Map.Entry<?, ?> next() {
            Object v = mIter.next();
            return new SimpleEntry<Integer, Object>( mIndex++, v );
        }

        public void remove() {
            mIter.remove();
            mIndex--;
        }

    }


    static final class ToEntryIterator implements Iterator<Map.Entry<?, ?>> {

        private final Iterator<?> mIter;


        ToEntryIterator( Iterator<?> iter ) {
            mIter = iter;
        }


        public boolean hasNext() {
            return mIter.hasNext();
        }

        public Map.Entry<?, ?> next() {
            Object v = mIter.next();
            return new SimpleEntry<Object, Object>( v, v );
        }

        public void remove() {
            mIter.remove();
        }

    }


    static abstract class WrapperSet<E> extends AbstractSet<E> {

        final Collection<?> mContainer;


        WrapperSet( Collection<?> container ) {
            mContainer = container;
        }


        public void clear() {
            mContainer.clear();
        }

        public abstract boolean contains( Object o );

        public abstract Iterator<E> iterator();

        public abstract boolean remove( Object o );

        public int size() {
            return mContainer.size();
        }

        public Object[] toArray() {
            return mContainer.toArray();
        }

        public <T> T[] toArray( T[] a ) {
            return mContainer.toArray( a );
        }

    }


    static final class ListIndexSet extends WrapperSet<Integer> {

        ListIndexSet( List list ) {
            super( list );
        }


        public boolean contains( Object obj ) {
            if( obj instanceof Integer ) {
                int idx = (Integer)obj;
                return idx >= 0 && idx < mContainer.size();
            }

            return false;
        }

        public boolean remove( Object obj ) {
            if( obj instanceof Integer )
                return false;

            int idx = (Integer)obj;

            if( idx >= 0 && idx < mContainer.size() ) {
                mContainer.remove( idx );
                return true;
            }

            return false;
        }

        public Iterator<Integer> iterator() {
            return new ToIndexIterator( mContainer.iterator() );
        }

    }


    static final class ListEntrySet extends WrapperSet<Map.Entry<?, ?>> {

        ListEntrySet( List list ) {
            super( list );
        }


        public boolean contains( Object obj ) {
            if( !(obj instanceof Map.Entry) )
                return false;

            Object key = ((Map.Entry)obj).getKey();
            if( !(key instanceof Integer) )
                return false;

            int idx = (Integer)key;
            if( idx < 0 || idx >= mContainer.size() )
                return false;

            Object v1 = ((List)mContainer).get( idx );
            Object v2 = ((Map.Entry)obj).getValue();

            return v1 == v2 || v1 != null && v1.equals( v2 );
        }

        public boolean remove( Object obj ) {
            if( !(obj instanceof Map.Entry) )
                return false;

            Object key = ((Map.Entry)obj).getKey();
            if( !(key instanceof Integer) )
                return false;

            int idx = (Integer)key;
            if( idx < 0 || idx >= mContainer.size() )
                return false;

            Object v1 = ((List)mContainer).get( idx );
            Object v2 = ((Map.Entry)obj).getValue();

            if( v1 == v2 || v1 != null && v1.equals( v2 ) ) {
                ((List)mContainer).remove( idx );
                return true;
            }

            return false;
        }

        public Iterator<Map.Entry<?, ?>> iterator() {
            return new ToIndexEntryIterator( mContainer.iterator() );
        }

    }



    // **************************************************************
    // Deprecated graveyard.
    // **************************************************************

    /**
     * @deprecated Use values(keys).iterator() instead.
     *
     *             <p>
     *             Gets iterator over values at a location in the blob. Iterator
     *             stays at specified level and does not descennd. <br/>
     *             For <i>maps</i>, equivalent to
     *             <code>values().iterator()</code>. <br/>
     *             For <i>collections</i>, equivalent to <code>iterator()</code>
     *             . <br/>
     *             Otherwise, an empty iterator is returned.
     *
     * @param keys
     *            Location in blob over which to iterate.
     * @return iterator over values
     */
    public Iterator<?> iterator( Object... keys ) {
        return values( keys ).iterator();
    }

    /**
     * @deprecated Use entrySet(keys).iterator() instead.
     *             <p>
     *             Gets iterator over values at a location in the blob. Iterator
     *             stays at specified level and does not descennd. <br/>
     *             For <i>maps</i>, equivalent to
     *             <code>values().iterator()</code>. <br/>
     *             For <i>collections</i>, equivalent to <code>iterator()</code>
     *             . <br/>
     *             Otherwise, an empty iterator is returned.
     *
     * @param keys
     *            Location in blob over which to iterate.
     * @return iterator over values
     */
    public Iterator<Map.Entry<?, ?>> entryIterator( Object... keys ) {
        return entrySet( keys ).iterator();
    }



    /**
     * Although convenient, the string escaping on this is very primitive and may give you corrupted JSON.
     */
    @Deprecated
    public String toJson() {
        StringBuilder builder = new StringBuilder();
        try {
            json( this, builder );
        } catch( IOException e ) {
            e.printStackTrace();
            return null;
        }
        return builder.toString();
    }

    @Deprecated
    public void print( StringBuilder buf, String indent ) {
        format( indent, buf );
    }

    @Deprecated
    public String toYamlString() {
        return toYaml();
    }

    @Deprecated
    public String toJsonString() {
        return toJson();
    }

    @Deprecated
    public void saveToYaml( File outputFile ) throws IOException {
        writeYaml( outputFile );
    }

    @Deprecated
    public void saveToYaml( Writer writer ) throws IOException {
        writeYaml( writer );
    }

    @Deprecated
    public static Blob loadFromYaml( File inputFile ) throws IOException {
        return readYaml( inputFile );
    }

    @Deprecated
    public static Blob loadFromYaml( URL url ) throws IOException {
        return readYaml( url );
    }

    @Deprecated
    public static Blob loadFromYaml( Reader reader ) throws IOException {
        return readYaml( reader );
    }

    @Deprecated
    public static Blob loadFromYaml( String yamlText ) throws IOException {
        return readYaml( yamlText );
    }

    @Deprecated
    private void json( Object data, StringBuilder buf ) throws IOException {
        if( data == null ) {
            buf.append( "null" );

        } else if( data instanceof Boolean ) {
            buf.append( ((Boolean)data).toString() );

        } else if( data instanceof Map ) {
            Map<Object, Object> map = (Map<Object, Object>)data;

            if( map.size() == 0 ) {
                buf.append( "{}" );
                return;
            }

            buf.append( "{" );
            for( Iterator<Map.Entry<Object, Object>> it = map.entrySet().iterator(); it.hasNext(); ) {
                Entry<Object, Object> e = it.next();
                Object key = e.getKey();
                if( !((key instanceof String) || (key instanceof Number)) )
                    throw new IOException( "Map keys must be strings. Found key of type " + key.getClass().getName() );
                json( key.toString(), buf );
                buf.append( ":" );
                json( e.getValue(), buf );
                if( it.hasNext() )
                    buf.append( "," );
            }
            buf.append( "}" );

        } else if( data instanceof Collection ) {
            Collection<Object> c = (Collection<Object>)data;

            if( c.size() == 0 ) {
                buf.append( "[]" );
                return;
            }

            buf.append( "[" );
            for( Iterator<Object> it = c.iterator(); it.hasNext(); ) {
                json( it.next(), buf );
                if( it.hasNext() )
                    buf.append( "," );
            }
            buf.append( "]" );

        } else if( data instanceof Blob ) {
            json( ((Blob)data).mRoot, buf );

        } else if( data instanceof String ) {
            String s = ((String)data)
                    .replace( "\\", "\\\\" )
                    .replace( "\"", "\\\"" )
                    .replace( "\b", "\\b" )
                    .replace( "\f", "\\f" )
                    .replace( "\n", "\\n" )
                    .replace( "\r", "\\r" )
                    .replace( "\t", "\\t" );

            buf.append( '"' );
            buf.append( s );
            buf.append( '"' );

        } else if( data instanceof Number ) {
            buf.append( data.toString() );

        } else {
            throw new IOException( "Not a valid json type: " + data.getClass().getName() );
        }
    }

}
