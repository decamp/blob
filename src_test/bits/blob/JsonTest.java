package bits.blob;

import java.util.ArrayList;

import bits.blob.Blob;


/*
 * Created by: kubat
 * © Feb 12, 2011, MIT Media Lab
 */
public class JsonTest {

    /**
     * @param args
     */
    public static void main( String[] args ) {
        Blob b = new Blob();

        b.put( "first", 0, "a" );
        b.put( "first", 1, "b" );
        b.put( "first", 2, "c" );
        b.add( "second", "a", 1 );
        b.add( "second", "a", 2f );
        b.add( "second", "b", 3d );
        b.add( "second", "b", 4l );
        b.add( "second", "b", 4.5e33 );
        b.put( "third", "zzzz" );
        b.put( "fourth", null );
        b.put( "fifth", "\"haha,\" she said." );
        b.put( "sixth", "backslash: \\\nquote: \"" );
        b.put( 8, "eighth" );

        System.out.println( b.toJson() );
        System.out.println( b.slice( "second" ).toJson() );

        b.put( new ArrayList<Object>(), "value" );
        
        // This should fail
        //System.out.println( b.toJson() );
        
        b.clear();
        b.add( "first" );
        b.add( 2 );
        b.add( new Blob() );
        b.put( 2, "hello", "world" );
        b.add( new Blob() );
        b.add( 3, 1 );
        b.add( 3, 2 );
        b.add( 3, true );
        b.add( 3, false );
        b.add( 3, null );
        System.out.println( b.toJson() );
    }

}
