package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    //https://developer.android.com/training/data-storage/sqlite.html
    //instantiating subclass of SQLiteOpenHelper:
    KeyValueTableDBHelper dbHelper;
    static final int SERVER_PORT=10000;
    static final String global_identifier="*";
    static final String local_identifier="@";
    static final int AVD0_PORT=11108;
    int clientPort=0;
    SimpleDhtHelper helper=new SimpleDhtHelper();
    static  final String[] operations= new String[]{"JOIN","INSERT","QUERY","DELETE"};
    List<String> nodes = new ArrayList<String>();
    List<ChordNode> chordNodes=new ArrayList<ChordNode>();
    HashMap<Integer,Integer> emulatorMap=new HashMap<Integer, Integer>();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

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
        /*
        Log.d("insert","db created");
        // Gets the data repository in write mode
        dbHelper = new KeyValueTableDBHelper(getContext());

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        Log.d("insert","db created");
        //https://stackoverflow.com/questions/11686645/android-sqlite-insert-update-table-columns-to-keep-the-identifieradd .
        //https://developer.android.com/training/data-htmlstorage/sqlite.
        // Insert the new row, returning the primary key value of the new row
        long newRowId = db.insertWithOnConflict(KeyValueTableContract.KeyValueTableEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);

        Log.d("insert", values.toString());
        return uri;*/
        return uri;
    }

    @Override
    public boolean onCreate() {
        emulatorMap=helper.fillHashMap(emulatorMap);
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
        Log.d("onCreate",Integer.toString(clientPort));

        //If the port is not avd0 send a join request

        if(clientPort!=AVD0_PORT){
            int emulator=emulatorMap.get(AVD0_PORT);
            String hashed_emulator= null;
            try {
                hashed_emulator = genHash(Integer.toString(emulator));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.d("Emu",hashed_emulator+" "+emulator);
            nodes.add(hashed_emulator);
            String port=Integer.toString(clientPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,port+":"+"JOIN");
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
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
        int sequence_number=0;
        int i=0;
        Uri providerUri= Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Log.d("Server","Message in Server");
                    Socket socket = serverSocket.accept();
                    Log.d("Server", "Connection Successful");
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String receivedMessage=inputStream.readUTF();
                    Log.d("Server",receivedMessage);
                    String[] messages=receivedMessage.split(":");
                    int receivedPort=Integer.parseInt(messages[0]);
                    String operation=messages[1];
                    String successor=messages[2];
                    String predecessor=messages[3];
                    Log.d("Server",messages[0]);
                    Log.d("Operation",messages[1]);
                    if(operations[0].equals(operation))
                    {
                        int emulator=emulatorMap.get(receivedPort);
                        String hashed_emulator=genHash(Integer.toString(emulator));
                        Log.d("Emu",hashed_emulator+" "+emulator);
                        nodes.add(hashed_emulator);
                        i++;
                        Collections.sort(nodes,new CustomComparator());
                        if(i==4)
                        {
                            Log.d("zzzzz",nodes.get(0));
                            Log.d("zzzzz",nodes.get(1));
                            Log.d("zzzzz",nodes.get(2));
                            Log.d("zzzzz",nodes.get(3));
                            Log.d("zzzzz",nodes.get(4));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ex.getMessage();
            }

            return null;
        }


        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            return;
        }

        protected void setSuccessorAndPredecessor(String hash_id,String emulator){
           // https://stackoverflow.com/questions/2784514/sort-arraylist-of-custom-objects-by-property

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
                    String successor=" ";
                    String predecessor=" ";
                    Log.d("Client",requested_message+" wants to connect");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), AVD0_PORT);
                    Log.d("Client","Socket Created");
                    DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                    String client_message=requested_message+":"+successor+":"+predecessor;
                    Log.d("Message to send",client_message);
                    outputStream.writeUTF(client_message);
                    outputStream.flush();

                } catch (Exception e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                }
            }
            else{
                try {

                }
                catch (Exception ex)
                {

                }
            }

        return null;
        }
    }
}
class CustomComparator implements Comparator<String> {
    @Override
    public int compare(String hash_id_1, String hash_id_2) {
        return hash_id_1.compareTo(hash_id_2);
    }
}
