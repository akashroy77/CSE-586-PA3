package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT=10000;
    static final String global_identifier="*";
    static final String local_identifier="@";
    static final int AVD0_PORT=11108;
    int clientPort=0;
    SimpleDhtHelper helper=new SimpleDhtHelper();
    HashMap<Integer,Integer> successor_map=new HashMap<Integer, Integer>();
    HashMap<Integer,Integer> predecessor_map=new HashMap<Integer, Integer>();
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
        return null;
    }

    @Override
    public boolean onCreate() {
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
            Log.e(TAG, "Can't create a ServerSocket now");
        }

        //Filling up the Maps with Default Ports
        try {
            successor_map=helper.fillHashMap(successor_map);
            predecessor_map=helper.fillHashMap(predecessor_map);
        }
        catch (Exception ex){
            ex.getMessage();
        }

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        clientPort=Integer.parseInt(myPort);
        Log.d("onCreate",Integer.toString(clientPort));

        //If the port is not avd0 send a join request
        if(clientPort!=AVD0_PORT){
            String port=Integer.toString(clientPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,port);
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
        Uri providerUri= Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    Log.d("Server:", "Connection Successful");
                    DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                    String receivedMessage=inputStream.readUTF();
                    Log.d("Server",receivedMessage);
                    String[] messages=receivedMessage.split(":");
                    int receivedPort=Integer.parseInt(messages[1]);
                    Log.d("Server",messages[1]);

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
            String joining_port=msgs[0];
            Log.d("Client",joining_port+" wants to connect");
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        AVD0_PORT);
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                String output="Join"+":"+joining_port;
                outputStream.writeUTF(output);
                outputStream.flush();

            } catch (Exception e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }
        return null;
        }
    }
}
