package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

//https://developer.android.com/training/data-storage/sqlite.html
public class KeyValueTableDBHelper extends SQLiteOpenHelper {
    //CREATE method for the database table
    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + KeyValueTableContract.KeyValueTableEntry.TABLE_NAME + " ("+
                    KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY + " TEXT PRIMARY KEY," +
                    KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE + " TEXT)";

    ////DELETE method for the database table
    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + KeyValueTableContract.KeyValueTableEntry.TABLE_NAME;

    //https://developer.android.com/training/data-storage/sqlite.html
    // If we make changes in the DB then we need to increment the database version.
    public static final int DATABASE_VERSION;
    public static final String DATABASE_NAME = "KeyValueStorage.db";

    //Randomizing the DATABASE_VERSION so that it won't be same for two consecutive run
    // and it will wipe the old data from all the avds
    static{
        DATABASE_VERSION = (int)(Math.random()*1000);
        Log.d("DBHelper","current db version is :"+DATABASE_VERSION);
    }
    public KeyValueTableDBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db)
    {
        db.execSQL(SQL_CREATE_ENTRIES);

    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);

    }
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }

}

