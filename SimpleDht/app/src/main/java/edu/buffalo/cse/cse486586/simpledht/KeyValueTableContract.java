package edu.buffalo.cse.cse486586.simpledht;

//https://developer.android.com/training/data-storage/sqlite.html
import android.provider.BaseColumns;

//Creating the contract for the KeyValueStorageDatabase and this class defines names for URIs, tables, and columns

public class KeyValueTableContract {
    // To prevent someone from accidentally instantiating the contract class,
    // making the constructor private.
    private KeyValueTableContract() {}

    /* Inner class that defines the table contents */
    public static class KeyValueTableEntry implements BaseColumns
    {
        public static final String TABLE_NAME = "keyValueTable";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
    }
}
