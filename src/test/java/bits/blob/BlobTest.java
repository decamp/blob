/*
 * Copyright (c) 2014. Massachusetts Institute of Technology
 * Released under the BSD 2-Clause License
 * http://opensource.org/licenses/BSD-2-Clause
 */ 
package bits.blob;

import java.io.File;
import java.io.IOException;

import org.junit.*;

import bits.blob.Blob;
import junit.framework.JUnit4TestAdapter;
import static org.junit.Assert.*;


/**
 * @author Philip DeCamp
 */
public class BlobTest {

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter( BlobTest.class );
    }


    @Test public void testPut() {
        Blob b = new Blob();
        b.put( "a", "b", "c", "happy" );
        b.put( "b", 0, 2, "sad" );

        assertEquals( "happy", b.get( "a", "b", "c" ) );
        assertEquals( "sad", b.get( "b", 0, 2 ) );

        b.put( "oops" );
        assertEquals( b.get(), "oops" );

        b.clear();
        b.put( 0, 0, "whatever" );
        b.put( 0, "happy", "whatevs" );
    }


    @Test public void testAdd() {
        Blob b = new Blob();
        b.add( "a", "b", "c", "happy" );
        b.add( "a", "b", "d" );
        b.add( "b", -1, 2, "sad" );

        assertEquals( "happy", b.get( "a", "b", "c", 0 ) );
        assertTrue( b.getMap( "a", "b" ).containsKey( "d" ) );
        assertTrue( b.getMap( "a", "b" ).containsValue( "d" ) );
    }


    @Test public void testListPadding() {
        Blob b = new Blob();
        b.put( 0, "whatever" );
        assertEquals( b.size(), 1 );
        b.put( 2, "killjoy" );
        assertEquals( b.size(), 3 );

        // Here, rather than adding a million null values to the list,
        // the blob should convert the list to a map.
        b.put( 1000000, "tomap" );
        assertEquals( b.size(), 4 );
    }


    @Test public void testRemove() {
        Blob b = new Blob();
        b.put( "a", "b", "c", "happy" );
        b.put( "b", -1, 2, "sad" );

        assertEquals( "happy", b.remove( "a", "b", "c" ) );
        assertEquals( "sad", b.remove( "b", -1, 2 ) );
        assertNull( b.remove( "a", "b", "c" ) );
        assertNull( b.remove( "b", "-1", 2 ) );
    }


    @Test public void testContains() {
        Blob b = new Blob();
        b.put( "a", "b", "c", "happy" );
        b.put( "b", -1, 2, "sad" );

        assertTrue( b.containsKey( "a" ) );
        assertTrue( b.containsKey( "b", -1 ) );
        assertTrue( b.containsValue( "a", "b", "happy" ) );
        assertTrue( b.containsValue( "b", -1, "sad" ) );
    }


    @Test public void testRecursivePut() {
        Blob b1 = new Blob();
        Blob b2 = new Blob();
        Blob b3 = new Blob();

        b1.put( b2 );
        b1.put( "a", "happy" );

        b3.put( "b", "sad" );

        b1.put( "d", "d", b3 );
        b1.put( "d", "d", "c", "woot" );


        assertNull( b1.getType( Blob.class ) );
        assertEquals( "happy", b1.get( "a" ) );
        assertNull( "happy", b2.get( "a" ) );
        assertEquals( "sad", b1.get( "d", "d", "b" ) );
        assertEquals( "sad", b3.get( "b" ) );
        assertEquals( "woot", b1.get( "d", "d", "c" ) );
        assertNull( b1.getType( Blob.class, "d", "d" ) );
    }


    @Test public void testRecursiveAdd() {
        Blob b1 = new Blob();
        Blob b2 = new Blob();
        Blob b3 = new Blob();

        b1.add( b2 );
        b1.add( 0, b3 );
        b1.add( 0, 0, "a", "happy" );
        b1.add( "sad" );
        b1.add( 0, "woot" );

        assertNull( b1.getType( Blob.class, 0 ) );
        assertNull( b1.getType( Blob.class, 0, 0 ) );
        assertEquals( "happy", b1.get( 0, 0, "a", 0 ) );
        assertEquals( "sad", b1.get( 1 ) );
        assertEquals( "woot", b1.get( 0, 1 ) );

        b1.put( "closet", "door" );

        assertEquals( "happy", b1.get( 0, 0, "a", 0 ) );
        assertEquals( "sad", b1.get( 1 ) );
        assertEquals( "woot", b1.get( 0, 1 ) );
    }


    @Test public void testSlice() {
        Blob b1 = new Blob();
        Blob b2 = b1.slice( "s", "t" );

        b2.put( "x", 4 );

        assertEquals( 4, b1.get( "s", "t", "x" ) );
        assertEquals( 1, b1.size( "s" ) );

        b2.clear();

        assertNotNull( b1.get( "s" ) );
        assertEquals( 0, b1.size( "s" ) );

    }


    @Test public void testSliceForceGet() {
        Blob a = new Blob();
        a.put( "x", "y", "z" );
        Blob b = a.slice( "x" );

        try {
            b.forceGetString( "n" );
            assertTrue( "Failed to throw appropriate exception", false );
        } catch( IOException ex ) {
            String m = ex.getMessage();
            assertTrue( m.startsWith( "x:n" ) );
        }

    }



    public void testLoad() {
        try {
            Blob b = Blob.readYaml( new File( "lib/snakeyaml/src/test/resources/specification/example2_3.yaml" ) );
            System.out.println( b );
            System.out.println( b.toYaml() );
        } catch( Exception ex ) {
            ex.printStackTrace();
            assertTrue( ex.getMessage(), false );
        }

    }

}
