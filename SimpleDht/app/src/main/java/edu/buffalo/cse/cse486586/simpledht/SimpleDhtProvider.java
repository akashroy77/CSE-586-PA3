package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.IllegalCharsetNameException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.AtomicFile;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    //https://developer.android.com/training/data-storage/sqlite.html
    //instantiating subclass of SQLiteOpenHelper:
    KeyValueTableDBHelper dbHelper;
    ContentResolver contentResolver;
    static final int SERVER_PORT=10000;
    static final String global_identifier="*";
    static final String local_identifier="@";
    static final int AVD0_PORT=11108;
    int clientPort=0;
    static  final String[] operations= new String[]{"JOIN","SUCC_SEQUENCE","PRED_SEQUENCE","INSERT","ALL_QUERY","SINGLE_QUERY","QUERY_RESPONSE","ALL_DELETE","SINGLE_DELETE"};
    List<Integer> nodes = new ArrayList<Integer>();
    List<String> hashedNodes=new ArrayList<String>();
    HashMap<Integer,Integer> emulatorMap=new HashMap<Integer, Integer>();
    String outputQuery="";
    //Successor
    String sequenceSucc=" ";
    //Predecessor
    String sequencePred=" ";
    String hashedPort=" ";
    //Sema Variable
    boolean wait=false;
    Cursor cursor=null;
    MatrixCursor globalCursor=null;
    //Sema Variable
    AtomicBoolean flag=new AtomicBoolean(true);
    Uri globalUri=Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    int count=0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        // Gets the data repository in write mode
        // https://stackoverflow.com/questions/9599741/how-to-delete-all-records-from-table-in-sqlite-with-android
        int response=0;
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String[] deleteType=selection.split(":");
        Log.d("Delete","In Delete");
        Cursor cursor=null;
        try {
            if (onlyAVD()) {
                if (selection.equals(local_identifier) || selection.equals(global_identifier)) {
                    Log.d("Delete", "here");
                    response = db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null);
                }
                else {
                    // Gets the data repository in write mode
                    String[] whereCondition = {selection};
                    response = db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY + "=?", whereCondition);
                }
                return response;
            }
            else if(selection.equals(local_identifier)){
                Log.d("Delete","Deleting local AVD");
                response = db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null);
                return response;
            }
            else if(selection.equals(global_identifier)){
                Log.d("Delete","Deleting my own query");
                response = db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null);
                String deleteQuery=clientPort+":"+"ALL_DELETE"+":"+sequenceSucc+":"+"REPEAT";
                Log.d("Delete",deleteQuery);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteQuery);
                return response;
            }
            else if(waitingForOthersDeletion(deleteType)){
                // Chord has reached its root node
                if(rootOfChord(deleteType)){
                    Log.d("Delete","All Delete are complete");
                    return 1;
                }
                else {
                    // Delete Next Successor's Query
                    db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null);
                    String deleteQuery=deleteType[0]+":"+"ALL_DELETE"+":"+sequenceSucc+":"+"REPEAT";
                    Log.d("Delete",deleteQuery);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteQuery);
                }
            }
            else {
                cursor=query(globalUri,null,selection,null,null,null);
                Log.d("Response", DatabaseUtils.dumpCursorToString(cursor));
                try {
                    if (cursor != null && cursor.getCount() > 0) {
                        Log.d("Found Delete", "Not Null Cursor");
                        if (cursor.getColumnCount() == 3) {
                            Log.d("Returning Null","Null");
                            return 0;
                        } else {
                            response = db.delete(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null);
                            return response;
                        }
                    } else {
                        // Forwarding Delete
                        String deleteQuery = clientPort + ":" + "LOCAL_DELETE" + ":" + sequenceSucc;
                        Log.d("Successor Delete", deleteQuery);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteQuery);
                        Thread.sleep(1000);
                    }
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return 0;
    }

    // All Delete Checkings
    // ---------------------------------------------------------------------------------
    //Only One AVD Exist in the system
    private boolean onlyAVD() throws NoSuchAlgorithmException {
        if(sequencePred.equals(" ") || sequenceSucc.equals(" ") || genHash(sequencePred).equals(hashedPort) && genHash(sequenceSucc).equals(hashedPort)){
            return true;
        }
        return false;
    }

    //* Deletion (Waiting for all the AVD to Delete)
    private boolean waitingForOthersDeletion(String[] deleteType){
        if(deleteType.length>3 && deleteType[3].equals("REPEAT")){
            return true;
        }
        return false;
    }

    private boolean rootOfChord(String deleteType[]){
        if(deleteType[0].equals(clientPort)){
            return true;
        }
        return false;
    }

    //---------------------------------------------------------------------------------

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        /*
         * SQLite. But this is not a requirement. You can use other storage options, such
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * For actual storage, you can use any option. If you know how to use SQL, then you can useas the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        // Gets the data repository in write mode
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        String key=(String)values.get("key");
        String value=(String)values.get("value");
        String hashedKey=null;
        Log.d("Insert",key);
        try {
            hashedKey=genHash(key);
            Log.d("Seq",sequencePred);
            Log.d("Seq",sequenceSucc);

            if(isThisMyKey(hashedKey)){
                db.insertWithOnConflict(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.d("INSERT","Inserted in Same AVD "+hashedPort+" : "+key);
            }
            else if(hashedKey.compareTo(hashedPort)>0){
                String insertMessage=clientPort+":"+"INSERT"+":"+sequenceSucc+":"+key+":"+value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insertMessage);
            }
            // General Situation . My Key
            else if(hashedKey.compareTo(genHash(sequencePred))>0 && hashedKey.compareTo(hashedPort)<=0){
                db.insertWithOnConflict(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.d("INSERT","Inserted in Same AVD "+hashedPort+" : "+key);
            }
            else {
                String insertMessage=clientPort+":"+"INSERT"+":"+sequenceSucc+":"+key+":"+value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insertMessage);
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return uri;
    }

    // All Insert Checking
    // --------------------------------------------------------------
    private boolean isThisMyKey(String hashedKey){
        try {
            // I am the only AVD Here
            if (sequencePred.equals(" ") || sequenceSucc.equals(" ") || genHash(sequencePred).equals(hashedPort) && genHash(sequenceSucc).equals(hashedPort)){
                return true;
            }
            // Key Belongs to the Last Node of the Chord
            else if(genHash(sequencePred).compareTo(hashedKey)<0 && hashedKey.compareTo(hashedPort)>=0 && genHash(sequencePred).compareTo(hashedPort)>0){
                return true;
            }
            // Key Belongs to the Last Node of the Chord
            else if(genHash(sequencePred).compareTo(hashedKey)>0 && hashedKey.compareTo(hashedPort)<=0 && genHash(sequencePred).compareTo(hashedPort)>0){
                return true;
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return false;
    }
    //-------------------------------------------------------------------

    @Override
    public boolean onCreate() {
        dbHelper = new KeyValueTableDBHelper(getContext());
        contentResolver=(this.getContext()).getContentResolver();
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            Log.d("ServerSocket","Creating a Server Socket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.d("AVD",portStr);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        clientPort=Integer.parseInt(myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort+":"+"JOIN");
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        //https://developer.android.com/training/data-storage/sqlite.html
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder=new SQLiteQueryBuilder();
        queryBuilder.setTables(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME);
        String[] queryType=selection.split(":");
        Log.d("Select",selection);

        try {
            if (onlyAVD()) {
                cursor=null;
                if (selection.equals(local_identifier) || selection.equals(global_identifier)) {
                    Log.d("Query", "here");
                    cursor = db.query(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null, null, null, null, null, null);
                    Log.d("query", DatabaseUtils.dumpCursorToString(cursor));
                }
                else {
                    cursor = queryBuilder.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
                }
                return cursor;
            }
            else if(selection.equals(global_identifier)){
                String queryResult=" ";
                String inputQuery=" ";
                String queryMessage=" ";
                Log.d("Query For Own AVD", "here");
                try {
                    cursor = db.query(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null, null, null, null, null, null);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
                Log.d("Cursor",Integer.toString(cursor.getCount()));
                Log.d("query", DatabaseUtils.dumpCursorToString(cursor));
                //https://stackoverflow.com/questions/7420783/androids-sqlite-how-to-turn-cursors-into-strings
                //https://developer.android.com/reference/android/database/sqlite/SQLiteCursor
                try {
                    if (cursor.moveToFirst()) {
                        Log.d("Query", "Converting Cursor");
                        do {
                            queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY)) + "//";
                            queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE)) + "%%";
                            Log.d("Query", queryResult);
                        } while (cursor.moveToNext());
                        inputQuery = queryResult.substring(0, queryResult.length() - 2);
                    }
                    queryMessage = clientPort + ":" + "ALL_QUERY" + ":" + sequenceSucc + ":" + "ALL_QUERY" + ":" + inputQuery;
                    Log.d("Query", queryMessage);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
                try {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessage);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
                try {
                    if (flag.get()) {
                        Log.d("Waiting", Boolean.toString(flag.get()));
                        Log.d("Wait","Waiting Here");
                        Thread.sleep(8000);
                    }
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }

                return globalCursor;
            }
            else if(waitingForOthersQuery(queryType)){
                globalCursor=null;
                Log.d("Query","Inside Global Query");
                //Chord has come back to its root port
                if(queryType[0].equals(Integer.toString(clientPort))) {
                    Log.d("Query","Return All the Query Received");
                    // https://stackoverflow.com/questions/28936424/converting-multidimentional-string-array-to-cursor
                    String keyColumn= KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY;
                    String valueColumn= KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE;
                    String[] columns = new String[] { keyColumn,valueColumn};
                    try {
                        String allValues = queryType[4];
                        Log.d("Query", allValues);
                        String[] nodeQueries = allValues.split(" DELIMETER ");
                        MatrixCursor matrixCursor = new MatrixCursor(columns);
                        for (String entry : nodeQueries) {
                            entry=entry.trim();
                            Log.d("Query1", entry);
                            if (!entry.equals(" ") && !entry.equals(null) && !entry.equals("")) {
                                String keyValue[] = entry.split("%%");
                                for (String subentry : keyValue) {
                                    Log.d("Query2", subentry);
                                    subentry=subentry.trim();
                                    if(!subentry.equals(" ") && !subentry.equals("")) {
                                        String rows[] = subentry.split("//");
                                        String[] outputs = new String[]{rows[0].trim(), rows[1].trim()};
                                        matrixCursor.addRow(outputs);
                                    }
                                }
                            }
                        }
                        globalCursor = matrixCursor;
                        cursor.close();
                        flag.set(false);
                    }
                    catch (Exception ex){
                        ex.printStackTrace();
                    }
                    //Log.d("query", DatabaseUtils.dumpCursorToString(cursor));
                    //return matrixCursor;
                }
                else {
                    String queryResult=" ";
                    String inputQuery=" ";
                    String queryMessage=" ";
                    Log.d("Querying Next Successor", "here");
                    cursor=query(globalUri,null,"@",null,null,null);
                    Log.d("query", Integer.toString(cursor.getCount()));
                    Log.d("query", DatabaseUtils.dumpCursorToString(cursor));
                    //https://stackoverflow.com/questions/7420783/androids-sqlite-how-to-turn-cursors-into-strings
                    //https://developer.android.com/reference/android/database/sqlite/SQLiteCursor
                    try {
                        if (cursor.moveToFirst()) {
                            Log.d("Query", "Converting Cursor");
                            do {
                                queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY)) + "//";
                                queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE)) + "%%";
                            }
                            while (cursor.moveToNext());
                            inputQuery = queryResult.substring(0, queryResult.length() - 2);
                        }
                        outputQuery = queryType[4] + " DELIMETER " + inputQuery;
                        Log.d("ABC", inputQuery);
                        queryMessage = queryType[0] + ":" + "ALL_QUERY" + ":" + sequenceSucc + ":" + "ALL_QUERY" + ":" + outputQuery;
                        Log.d("Sending", queryMessage);
                    }
                    catch (Exception ex){
                        ex.printStackTrace();
                    }
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessage);
                    }
                    catch (Exception ex){
                        ex.printStackTrace();
                    }
                }
            }
            else if(selection.equals(local_identifier)){
                cursor=null;
                Log.d("Query", "here");
                cursor = db.query(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, null, null, null, null, null, null);
                Log.d("query", DatabaseUtils.dumpCursorToString(cursor));
                return cursor;
            }
            else {
                Log.d("Query","Searching own AVD and then passing to others");
                Log.d("Query",selection);
                Log.d("Query",Integer.toString(queryType.length));
                // Normal Query
                // Pass it to other AVD
                if(queryType.length==1){
                    cursor=null;
                    Log.d("Query","Searching OWN AVD");
                    Log.d("QD",selection);
                    cursor = queryBuilder.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
                    Log.d("Response", DatabaseUtils.dumpCursorToString(cursor));
                    if(cursor.getCount()>0){
                        return cursor;
                    }
                    else {
                        Log.d("Query","Could not find in local AVD");
                        int localCount=count+1;
                        String queryMessage = clientPort + ":" + "SINGLE_QUERY" + ":" + sequenceSucc + ":" + "LOCAL_QUERY"+":"+selection+"#"+localCount;
                        Log.d("Query",queryMessage);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessage);
                        semaup();
                    }
                }
                else if(isThisNextAVD(queryType)){
                    if(queryType[3].equals("LOCAL_QUERY")){
                        String[] deleteString=queryType[4].split("#");
                        Log.d("DeleteString",deleteString[1]);
                        Log.d("DleteString",Integer.toString(nodes.size()));
                        String queryResult=" ";
                        String inputQuery=" ";
                        Log.d("Query","I am in the next AVD");
                        cursor = queryBuilder.query(db, null, "key=" + "'" + deleteString[0] + "'", null, null, null, null);
                        Log.d("Query", Integer.toString(cursor.getCount()));
                        try {
                            if (cursor.getCount() > 0) {
                                Log.d("Query", "Found it");
                                Log.d("Response", DatabaseUtils.dumpCursorToString(cursor));
                                if (cursor.moveToFirst()) {
                                    Log.d("Query", "Converting Cursor");
                                    do {
                                        queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY)) + ":";
                                        queryResult += cursor.getString(cursor.getColumnIndex(KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE)) + "%%";
                                    }
                                    while (cursor.moveToNext());
                                    Log.d("Sending", queryResult);
                                    inputQuery = queryResult.substring(0, queryResult.length() - 2);
                                }

                                Log.d("Query", inputQuery);
                                int sendingEmulator = Integer.parseInt(queryType[0]) / 2;
                                String queryMessage = queryType[0] + ":" + "QUERY_RESPONSE" + ":" + sendingEmulator + ":" + "RETURN" + ":" + inputQuery;
                                Log.d("Query", queryMessage);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessage);
                            }
                            else if (queryType[0].equals(Integer.toString(clientPort))) {
                                try {
                                    Log.d("Returning",deleteString[1]);
                                    semadown();
                                    MatrixCursor deleteCursor;
                                    String[] outputs=new String[]{"null","null","null"};
                                    String keyColumn = KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY;
                                    String valueColumn = KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE;
                                    String nullColumn="Null Value";
                                    String[] columns = new String[]{keyColumn, valueColumn,nullColumn};
                                    MatrixCursor matrixCursor = new MatrixCursor(columns);
                                    matrixCursor.addRow(outputs);
                                    cursor=matrixCursor;
                                    Log.d("Response", DatabaseUtils.dumpCursorToString(cursor));
                                }
                                catch (Exception ex){
                                    ex.printStackTrace();
                                }
                                return cursor;
                            }
                            else {
                                try {
                                    Log.d("Query", "Not Found so passing it to other" + " " + queryType[0]);
                                    int localCount = Integer.parseInt(deleteString[1]);
                                    localCount = localCount + 1;
                                    String queryMessage = queryType[0] + ":" + "SINGLE_QUERY" + ":" + sequenceSucc + ":" + "LOCAL_QUERY" + ":" + deleteString[0]+"#"+localCount;
                                    Log.d("Query", queryMessage);
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessage);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        }
                        catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                }
            }
        }

        catch (Exception ex){
            cursor = null;
            semadown();
        }
        if (isResponse(queryType)){
            Log.d("Query","In Response");
            try {
                String[] outputs = new String[]{queryType[4].trim(), queryType[5]};
                Log.d("Response",Integer.toString(queryType.length));
                Log.d("Response",queryType[4]);
                Log.d("Response",queryType[5]);
                // https://stackoverflow.com/questions/28936424/converting-mult5mentional-string-array-to-cursor
                String keyColumn = KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_KEY;
                String valueColumn = KeyValueTableContract.KeyValueTableEntry.COLUMN_NAME_VALUE;
                String[] columns = new String[]{keyColumn, valueColumn};
                MatrixCursor matrixCursor = new MatrixCursor(columns);
                matrixCursor.addRow(outputs);
                cursor=matrixCursor;
                Log.d("Response", DatabaseUtils.dumpCursorToString(matrixCursor));
            }
            catch (Exception ex){
                cursor = null;
                semadown();
                ex.printStackTrace();
                return cursor;

            }
        }
        if (wait) {
            wait = false;
        }
        return cursor;
    }

    // All Query Checking
    // -------------------------------------------------------------------------------------

    private void semaup(){
        while (true) {
            if(wait){
                break;
            }
        }
    }

    private void semadown(){
        wait=true;
    }

    private boolean waitingForOthersQuery(String[] queryType){
        if(queryType.length>2 && queryType[3].equals(operations[4])){
            return true;
        }
        return false;
    }

    private boolean isResponse(String[] queryType){
        if(queryType.length>1 && queryType[1].equals(operations[6])){
            return true;
        }
        return false;
    }

    private boolean isThisNextAVD(String[] queryType){
        if(queryType.length>1){
            return true;
        }
        return false;
    }

    //-------------------------------------------------------------------------------------

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     */

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        int i=0;
        Uri providerUri=Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Log.d("Server","Message in Server");
                    Log.d("Server","Trying to connect");
                    Socket socket = serverSocket.accept();
                    Log.d("Server", "Connection Successful");
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String receivedMessage=inputStream.readUTF();
                    Log.d("Server",receivedMessage);
                    String[] messages=receivedMessage.split(":");
                    //Port
                    int receivedPort=Integer.parseInt(messages[0]);
                    //Operation
                    String operation=messages[1];
                    //Emulator
                    int emulator=Integer.parseInt(messages[2]);
                    Log.d("Server1","Here");
                    Log.d("Server",messages[0]);
                    Log.d("Operation Here",messages[1]);
                    Log.d("For Emualator",messages[2]);
                    if(operations[0].equals(operation))
                    {
                        try{
                            String localSuccessor=" ";
                            String localPredecessor=" ";
                            String hashedEmu=genHash(Integer.toString(emulator));
                            Log.d("Emu",hashedEmu+" "+emulator);
                            nodes.add(emulator);
                            hashedNodes.add(hashedEmu);
                            i++;
                            Collections.sort(nodes,new CustomHashComparator());
                            int nodePosition=nodes.indexOf(emulator);
                            Log.d("Np",Integer.toString(nodePosition));
                            Log.d("Size",Integer.toString(nodes.size()));
                            //Set Successor
                            //Last Node of the Chord
                            if(nodePosition==nodes.size()-1){
                                localSuccessor=Integer.toString(nodes.get(0));
                            }
                            else {
                                localSuccessor=Integer.toString(nodes.get(nodePosition+1));
                                Log.d("Succ", Integer.toString(nodes.get(nodePosition+1)));
                            }
                            //Set Predecessor
                            //First Node of the Chord
                            if(nodePosition==0){
                                localPredecessor=Integer.toString(nodes.get(nodes.size()-1));
                                Log.d("pred",localPredecessor);
                            }
                            else {
                                localPredecessor=Integer.toString(nodes.get(nodePosition-1));
                            }
                            String clientSuccessorOutputString=emulator+":"+"SUCC_SEQUENCE"+":"+localSuccessor+":"+" ";
                            Log.d("Server","Sending Successor to Client"+clientSuccessorOutputString);
                            callClient(clientSuccessorOutputString);

                            String clientPredOutputString=emulator+":"+"PRED_SEQUENCE"+":"+ localPredecessor+":"+" ";
                            Log.d("Server","Sending Predecessor to Client"+clientPredOutputString);
                            callClient(clientPredOutputString);

                            String clientSelfSuccUpdate=localSuccessor+":"+"SUCC_SEQUENCE"+":"+ emulator+":"+"Reverse";
                            callClient(clientSelfSuccUpdate);
                            Log.d("Sending SelfSucc Update",clientSelfSuccUpdate);
                            String clientSelfPredUpdate=localPredecessor+":"+"PRED_SEQUENCE"+":"+emulator+":"+"Reverse";
                            callClient(clientSelfPredUpdate);
                            Log.d("Sending SelfPred Update",clientSelfPredUpdate);
                        }
                        catch (Exception ex){
                            ex.printStackTrace();
                        }
                    }
                    else if (operations[1].equals(operation)){
                        String localSuccessor=messages[0];
                        Log.d("Local Successor",messages[0]);
                        Log.d("Server","In Successor Sequence");
                        if(messages[3].equals("Reverse")){
                            sequenceSucc=localSuccessor;
                        }
                        else {
                            sequencePred=localSuccessor;
                        }
                        Log.d("Final P Final S",sequencePred+" "+sequenceSucc);
                    }
                    else if(operations[2].equals(operation)){
                        Log.d("Server","In Predecessor Sequence");
                        String localPredecessor=messages[0];
                        Log.d("Local Predecessor",messages[0]);
                        if(messages[3].equals("Reverse")){
                            sequencePred=localPredecessor;
                        }
                        else {
                            sequenceSucc = localPredecessor;
                        }
                        Log.d("Final P Final S",sequencePred+" "+sequenceSucc);
                    }
                    else if(operations[3].equals(operation)){
                        String key=messages[3];
                        String value=messages[4];
                        Log.d("Server","In Inside Sequence");
                        //Storing Value to the Database Using Content Provider
                        ContentValues keyValueToInsert = new ContentValues();
                        // Calling Insert
                        keyValueToInsert.put("key",key);
                        keyValueToInsert.put("value",value);
                        contentResolver.insert(providerUri,keyValueToInsert);
                    }
                    else if(operations[4].equals(operation)){
                        Log.d("Calling my query",receivedMessage);
                        query(providerUri,null,receivedMessage,null,null,null);
                    }
                    else if(operations[5].equals(operation)){
                        query(providerUri,null,receivedMessage+":",null,null,null);
                    }
                    else if(operations[6].equals(operation)){
                        query(providerUri,null,receivedMessage,null,null,null);
                        semadown();
                    }
                    else if(operations[7].equals(operation)){
                        delete(providerUri,receivedMessage,null);
                    }
                    else if(operations[8].equals(operation)){
                        delete(providerUri,receivedMessage,null);
                    }
                }
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }

            return null;
        }

        protected void callClient(String message){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,message);
        }
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String requested_message=msgs[0];
            String[] messages=requested_message.split((":"));
            String operation= messages[1];
            if(operation.equals(operations[0])){
                try {
                    sequenceSucc=" ";
                    sequencePred=" ";
                    String port=messages[0];
                    int emulator=Integer.parseInt(port)/2;
                    hashedPort=genHash(String.valueOf(emulator));
                    Log.d("Client",requested_message+" wants to connect");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), AVD0_PORT);
                    Log.d("Client","Socket Created");
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String client_message=port+":"+"JOIN"+":"+emulator;
                    Log.d("Message to send",client_message);
                    outputStream.writeUTF(client_message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if(operation.equals(operations[1])){
                try {
                    String successor=messages[2];
                    int sendingPort=Integer.parseInt(successor)*2;
                    String port= Integer.toString(sendingPort);
                    Log.d("Client",port);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream=new DataOutputStream(socket.getOutputStream());
                    String outputToServer=requested_message;
                    Log.d("Client Sending String",requested_message);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[2])){
                try {
                    String predecessor=messages[2];
                    Log.d("Client",predecessor);
                    int sendingPort=Integer.parseInt(predecessor)*2;
                    Log.d("Client",predecessor);
                    String port=Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream=new DataOutputStream(socket.getOutputStream());
                    String outputToServer=requested_message;
                    Log.d("Client Sending String",outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[3])){
                try {
                    String successor=messages[2];
                    int sendingPort=Integer.parseInt(successor)*2;
                    Log.d("Client",successor);
                    String port=Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream=new DataOutputStream(socket.getOutputStream());
                    String outputToServer=requested_message;
                    Log.d("Client Sending String",outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[4])){
                try {
                    String successor = messages[2];
                    int sendingPort = Integer.parseInt(successor) * 2;
                    Log.d("Client", successor);
                    String port = Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String outputToServer = requested_message;
                    Log.d("Client Sending String", outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[5])){
                try {
                    String successor = messages[2];
                    int sendingPort = Integer.parseInt(successor) * 2;
                    Log.d("Client", successor);
                    String port = Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String outputToServer = requested_message;
                    Log.d("Client Sending String", outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[6])){
                try {
                    String successor = messages[2];
                    int sendingPort = Integer.parseInt(successor) * 2;
                    Log.d("Client", successor);
                    String port = Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String outputToServer = requested_message;
                    Log.d("Client Sending String", outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[7])) {
                try {
                    String successor = messages[2];
                    int sendingPort = Integer.parseInt(successor) * 2;
                    Log.d("Client", successor);
                    String port = Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String outputToServer = requested_message;
                    Log.d("Client Sending String", outputToServer);
                    outputStream.writeUTF(outputToServer);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            else if(operation.equals(operations[8])){
                try {
                    String successor = messages[2];
                    int sendingPort = Integer.parseInt(successor) * 2;
                    Log.d("Client", successor);
                    String port = Integer.toString(sendingPort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String outputToServer = requested_message;
                    Log.d("Client Sending String", outputToServer);
                    outputStream.writeUTF(outputToServer);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            return null;
        }
    }
    class CustomHashComparator implements Comparator<Integer> {
        @Override
        public int compare(Integer lhs, Integer rhs) {
            String lhsHash = null;
            String rhsHash = null;
            try {
                lhsHash = genHash(Integer.toString(lhs));
                rhsHash = genHash(Integer.toString(rhs));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return lhsHash.compareTo(rhsHash);
        }
    }

    }
