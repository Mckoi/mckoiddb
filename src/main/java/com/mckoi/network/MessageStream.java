/**
 * com.mckoi.network.MessageStream  Nov 30, 2008
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with this program.  If not, see ( http://www.gnu.org/licenses/ ) or
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 * 
 * 
 */

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A sequence of command messages between a server and client, either function
 * calls or function replies.
 *
 * @author Tobias Downer
 */

public class MessageStream implements Iterable<Message>, ProcessResult {

  /**
   * The list of messages.
   */
  private ArrayList commands;
  
  /**
   * Constructor.
   */
  public MessageStream(int size) {
    commands = new ArrayList(size);
  }

  /**
   * Adds a message with the given name to the stream.
   */
  public void addMessage(String message_name) {
    if (message_name == null) {
      throw new NullPointerException();
    }
    if (message_name.equals(MESSAGE_CLOSE)) {
      throw new RuntimeException("Invalid message name");
    }
    commands.add(message_name);
  }
  
  /**
   * Closes the currently open message.
   */
  public void closeMessage() {
    commands.add(MESSAGE_CLOSE);
  }

  /**
   * Adds a general object to the message.
   */
  public void addObject(Object ob) {
    // Nulls not allowed,
    if (ob == null) {
      commands.add(null);
    }
    else if (ob instanceof Long) {
      addLong((Long) ob);
    }
    else if (ob instanceof Integer) {
      addInteger((Integer) ob);
    }
    else if (ob instanceof ServiceAddress) {
      addServiceAddress((ServiceAddress) ob);
    }
    else if (ob instanceof DataAddress) {
      addDataAddress((DataAddress) ob);
    }
    else if (ob instanceof BlockId) {
      addBlockId((BlockId) ob);
    }
    else if (ob instanceof String) {
      addString((String) ob);
    }
    else if (ob instanceof PathInfo) {
      addPathInfo((PathInfo) ob);
    }
    else if (ob instanceof BlockId[]) {
      addBlockIdArr((BlockId[]) ob);
    }
    else if (ob instanceof long[]) {
      addLongArray((long[]) ob);
    }
    else if (ob instanceof int[]) {
      addIntegerArray((int[]) ob);
    }
    else if (ob instanceof String[]) {
      addStringArr((String[]) ob);
    }
    else if (ob instanceof PathInfo[]) {
      addPathInfoArr((PathInfo[]) ob);
    }
    else if (ob instanceof ServiceAddress[]) {
      addServiceAddressArr((ServiceAddress[]) ob);
    }
    else if (ob instanceof DataAddress[]) {
      addDataAddressArr((DataAddress[]) ob);
    }
    else if (ob instanceof byte[]) {
      addBuf((byte[]) ob);
    }
    else if (ob instanceof ExternalThrowable) {
      addExternalThrowable((ExternalThrowable) ob);
    }
    else if (ob instanceof NodeSet) {
      addNodeSet((NodeSet) ob);
    }
    else {
      throw new RuntimeException("Unknown object type");
    }
  }

  /**
   * Adds a byte[] array to the message stream. It's assumed that the given
   * byte[] array is not mutated in any way for a stable message stream.
   */
  public void addBuf(byte[] buf) {
    commands.add(buf);
  }

  /**
   * Adds a NodeSet object to the message stream. It's assumed that node set
   * given is not mutated in any way for a stable message stream.
   */
  public void addNodeSet(NodeSet node_set) {
    commands.add(node_set);
  }

  /**
   * Adds a Long item to the message stream.
   */
  public void addLong(long v) {
    commands.add(v);
  }

  /**
   * Adds an array of long values.
   */
  public void addLongArray(long[] vs) {
    commands.add(vs);
  }

  /**
   * Adds an Integer item to the message stream.
   */
  public void addInteger(int v) {
    commands.add(v);
  }

  /**
   * Adds an array of integer values.
   */
  public void addIntegerArray(int[] vs) {
    commands.add(vs);
  }

  /**
   * Adds a String item to the message stream.
   */
  public void addString(String str) {
    if (str == null) {
      commands.add(null);
    }
    else {
      commands.add(new StringArg(str));
    }
  }

  /**
   * Adds a String array to the message stream.
   */
  public void addStringArr(String[] strs) {
    commands.add(strs);
  }

  /**
   * Adds a service address item to the message stream.
   */
  public void addServiceAddress(ServiceAddress s_addr) {
    commands.add(s_addr);
  }

  /**
   * Adds a DataAddress item to the message stream.
   */
  public void addDataAddress(DataAddress data_addr) {
    commands.add(data_addr);
  }

//  /**
//   * Adds a NodeReference item to the message stream.
//   */
//  public void addNodeReference(NodeReference node_reference) {
//    commands.add(node_reference);
//  }

  /**
   * Adds a BlockId item to the message stream.
   */
  public void addBlockId(BlockId block_id) {
    commands.add(block_id);
  }

  /**
   * Adds a BlockId array to the message stream.
   */
  public void addBlockIdArr(BlockId[] block_ids) {
    commands.add(block_ids);
  }

  /**
   * Adds a PathInfo item to the message stream.
   */
  public void addPathInfo(PathInfo path_info) {
    commands.add(path_info);
  }

  /**
   * Adds a PathInfo array item to the message stream.
   */
  public void addPathInfoArr(PathInfo[] path_infos) {
    commands.add(path_infos);
  }

  /**
   * Adds an external throwable item to the message stream.
   */
  public void addExternalThrowable(ExternalThrowable t) {
    commands.add(t);
  }

  /**
   * Adds a service address array item to the message stream.
   */
  public void addServiceAddressArr(ServiceAddress[] arr) {
    commands.add(arr);
  }

  /**
   * Adds a data address array to the message stream.
   */
  public void addDataAddressArr(DataAddress[] da_arr) {
    commands.add(da_arr);
  }
  
  /**
   * Returns a message iterator that iterates through the messages on this
   * stream, from the first message to the last.
   */
  public Iterator<Message> iterator() {
    return new MessageIterator();
  }

  /**
   * Writes a PathInfo object out to the given stream.
   */
  private static void writePathInfo(DataOutput dout, PathInfo path_info)
                                                           throws IOException {

    dout.writeUTF(path_info.getPathName());
    dout.writeUTF(path_info.formatString());

  }

  /**
   * Reads a PathInfo object in from the given stream.
   */
  private static PathInfo readPathInfo(DataInput din) throws IOException {

    String path_name = din.readUTF();
    String content = din.readUTF();

    return PathInfo.parseString(path_name, content);
  }

  /**
   * Writes this message stream out to an OutputStream. 'message_directory'
   * is a dictionary context for compressing the message strings, and must
   * remain constant through the connection context.
   */
  public void writeTo(DataOutput dout,
             HashMap<String, String> message_dictionary) throws IOException {

    dout.writeInt(commands.size());
    for (Object msg : commands) {
      // Null value handling,
      if (msg == null) {
        dout.writeByte(16);
      }
      else if (msg instanceof String) {
        if (msg.equals(MESSAGE_CLOSE)) {
          dout.writeByte(7);
        }
        else {
          // Is the message in the dictionary?
          String str_msg = (String) msg;
          String code = message_dictionary.get(str_msg);
          if (code == null) {
            int new_code = (message_dictionary.size() / 2) + 1;
            String new_code_str = Integer.toString(new_code);
            message_dictionary.put(str_msg, new_code_str);
            message_dictionary.put(new_code_str, str_msg);
            dout.writeByte(1);
            dout.writeByte(0);
            dout.writeShort(new_code);
            dout.writeUTF(str_msg);
          }
          else {
            int code_val = Integer.parseInt(code);
            dout.writeByte(1);
            dout.writeByte(1);
            dout.writeShort(code_val);
          }
        }
      }
      else if (msg instanceof Long) {
        dout.writeByte(2);
        dout.writeLong((Long) msg);
      }
      else if (msg instanceof Integer) {
        dout.writeByte(3);
        dout.writeInt((Integer) msg);
      }
      else if (msg instanceof byte[]) {
        dout.writeByte(4);
        byte[] buf = (byte[]) msg;
        dout.writeInt(buf.length);
        dout.write(buf, 0, buf.length);
      }
      else if (msg instanceof StringArg) {
        dout.writeByte(5);
        StringArg str_arg = (StringArg) msg;
        dout.writeUTF(str_arg.str_ob);
      }
      else if (msg instanceof long[]) {
        dout.writeByte(6);
        long[] arr = (long[]) msg;
        dout.writeInt(arr.length);
        for (int i = 0; i < arr.length; ++i) {
          dout.writeLong(arr[i]);
        }
      }
      else if (msg instanceof NodeSet) {
        dout.writeByte(17);
        if (msg instanceof SingleUncompressedNodeSet) {
          dout.writeByte(1);
        }
        else if (msg instanceof CompressedNodeSet) {
          dout.writeByte(2);
        }
        else {
          throw new RuntimeException("Unknown NodeSet type: " + msg.getClass());
        }
        NodeSet nset = (NodeSet) msg;
        // Write the node set,
        NodeReference[] arr = nset.getNodeIdSet();
        dout.writeInt(arr.length);
        for (int i = 0; i < arr.length; ++i) {
          NodeReference node_ref = arr[i];
          dout.writeLong(node_ref.getHighLong());
          dout.writeLong(node_ref.getLowLong());
        }
        // Write the binary encoding,
        nset.writeEncoded(dout);
      }
      else if (msg instanceof DataAddress) {
        dout.writeByte(9);
        DataAddress data_addr = (DataAddress) msg;
        dout.writeInt(data_addr.getDataId());
        BlockId block_id = data_addr.getBlockId();
        dout.writeLong(block_id.getHighLong());
        dout.writeLong(block_id.getLowLong());
      }
      else if (msg instanceof ExternalThrowable) {
        dout.writeByte(10);
        ExternalThrowable e = (ExternalThrowable) msg;
        dout.writeUTF(e.getClassName());
        dout.writeUTF(e.getMessage());
        dout.writeUTF(e.getStackTrace());
      }
      else if (msg instanceof ServiceAddress[]) {
        dout.writeByte(11);
        ServiceAddress[] arr = (ServiceAddress[]) msg;
        dout.writeInt(arr.length);
        for (ServiceAddress s : arr) {
          s.writeTo(dout);
        }
      }
      else if (msg instanceof DataAddress[]) {
        dout.writeByte(12);
        DataAddress[] arr = (DataAddress[]) msg;
        dout.writeInt(arr.length);
        for (DataAddress addr : arr) {
          dout.writeInt(addr.getDataId());
          BlockId block_id = addr.getBlockId();
          dout.writeLong(block_id.getHighLong());
          dout.writeLong(block_id.getLowLong());
        }
      }
      else if (msg instanceof ServiceAddress) {
        dout.writeByte(13);
        ((ServiceAddress) msg).writeTo(dout);
      }
      else if (msg instanceof String[]) {
        dout.writeByte(14);
        String[] arr = (String[]) msg;
        dout.writeInt(arr.length);
        for (String s : arr) {
          dout.writeUTF(s);
        }
      }
      else if (msg instanceof int[]) {
        dout.writeByte(15);
        int[] arr = (int[]) msg;
        dout.writeInt(arr.length);
        for (int v : arr) {
          dout.writeInt(v);
        }
      }

      else if (msg instanceof BlockId) {
        dout.writeByte(18);
        BlockId block_id = (BlockId) msg;
        dout.writeLong(block_id.getHighLong());
        dout.writeLong(block_id.getLowLong());
      }
      else if (msg instanceof BlockId[]) {
        dout.writeByte(19);
        BlockId[] arr = (BlockId[]) msg;
        dout.writeInt(arr.length);
        for (BlockId block_id : arr) {
          dout.writeLong(block_id.getHighLong());
          dout.writeLong(block_id.getLowLong());
        }
      }

      else if (msg instanceof PathInfo) {
        dout.writeByte(20);
        PathInfo path_info = (PathInfo) msg;
        writePathInfo(dout, path_info);
      }
      else if (msg instanceof PathInfo[]) {
        dout.writeByte(21);
        PathInfo[] path_infos = (PathInfo[]) msg;
        dout.writeInt(path_infos.length);
        for (PathInfo path_info : path_infos) {
          writePathInfo(dout, path_info);
        }
      }

      else {
        throw new RuntimeException("Unknown message object in list");
      }
    }
    // End of stream (for now).
    dout.writeByte(8);
  }

  /**
   * Reads the message stream in from an input stream.
   */
  public static MessageStream readFrom(DataInput din,
          HashMap<String, String> message_dictionary) throws IOException {

    int message_sz = din.readInt();
    MessageStream message_str = new MessageStream(message_sz);
    for (int i = 0; i < message_sz; ++i) {
      byte type = din.readByte();
//      System.out.println("type = " + type);
      if (type == 16) {
        // Nulls
        message_str.addDataAddress(null);
      }
      else if (type == 1) {
        // Open message,
        int cc = din.readByte();
        if (cc == 0) {
          // Message with code and with string,
          int code_val = (int) din.readShort();
          String code_val_str = Integer.toString(code_val);
          String message_name = din.readUTF();
          message_dictionary.put(message_name, code_val_str);
          message_dictionary.put(code_val_str, message_name);

          message_str.addMessage(message_name);
        }
        else if (cc == 1) {
          // Message with code and without string,
          int code_val = (int) din.readShort();
          String code_val_str = Integer.toString(code_val);
          String message_name = message_dictionary.get(code_val_str);

          message_str.addMessage(message_name);
        }
        else {
          throw new RuntimeException("Unknown format");
        }
      }
      else if (type == 2) {
        // Long argument
        message_str.addLong(din.readLong());
      }
      else if (type == 3) {
        // Integer argument
        message_str.addInteger(din.readInt());
      }
      else if (type == 4) {
        // byte[] array
        int sz = din.readInt();
        byte[] buf = new byte[sz];
        din.readFully(buf, 0, sz);
        message_str.addBuf(buf);
      }
      else if (type == 5) {
        // StringArg
        String str = din.readUTF();
        message_str.addString(str);
      }
      else if (type == 6) {
        // Long array
        int sz = din.readInt();
        long[] arr = new long[sz];
        for (int n = 0; n < sz; ++n) {
          arr[n] = din.readLong();
        }
        message_str.addLongArray(arr);
      }
      else if (type == 7) {
        // Close message,
        message_str.closeMessage();
      }
      else if (type == 9) {
        // DataAddress object
        int data_id = din.readInt();
        long block_id_h = din.readLong();
        long block_id_l = din.readLong();
        BlockId block_id = new BlockId(block_id_h, block_id_l);
        message_str.addDataAddress(new DataAddress(block_id, data_id));
      }
      else if (type == 10) {
        // ExternalThrowable object
        String class_name = din.readUTF();
        String message = din.readUTF();
        String stack_trace = din.readUTF();
        message_str.addExternalThrowable(
                     new ExternalThrowable(class_name, message, stack_trace));
      }
      else if (type == 11) {
        // ServiceAddress array,
        int sz = din.readInt();
        ServiceAddress[] arr = new ServiceAddress[sz];
        for (int n = 0; n < sz; ++n) {
          arr[n] = ServiceAddress.readFrom(din);
        }
        message_str.addServiceAddressArr(arr);
      }
      else if (type == 12) {
        // DataAddress array,
        int sz = din.readInt();
        DataAddress[] arr = new DataAddress[sz];
        for (int n = 0; n < sz; ++n) {
          int data_id = din.readInt();
          long block_id_h = din.readLong();
          long block_id_l = din.readLong();
          BlockId block_id = new BlockId(block_id_h, block_id_l);
          arr[n] = new DataAddress(block_id, data_id);
        }
        message_str.addDataAddressArr(arr);
      }
      else if (type == 13) {
        // ServiceAddress object,
        ServiceAddress saddr = ServiceAddress.readFrom(din);
        message_str.addServiceAddress(saddr);
      }
      else if (type == 14) {
        // String array
        int sz = din.readInt();
        String[] arr = new String[sz];
        for (int n = 0; n < sz; ++n) {
          String str = din.readUTF();
          arr[n] = str;
        }
        message_str.addStringArr(arr);
      }
      else if (type == 15) {
        // Integer array
        int sz = din.readInt();
        int[] arr = new int[sz];
        for (int n = 0; n < sz; ++n) {
          int v = din.readInt();
          arr[n] = v;
        }
        message_str.addIntegerArray(arr);
      }
      else if (type == 17) {
        byte node_set_type = din.readByte();
        // The node_ids list,
        int sz = din.readInt();
        NodeReference[] arr = new NodeReference[sz];
        for (int n = 0; n < sz; ++n) {
          long nr_high = din.readLong();
          long nr_low = din.readLong();
          arr[n] = new NodeReference(nr_high, nr_low);
        }
        // The binary encoding,
        sz = din.readInt();
        byte[] buf = new byte[sz];
        din.readFully(buf, 0, sz);
        // Make the node_set object type,
        if (node_set_type == 1) {
          // Uncompressed single,
          message_str.addNodeSet(new SingleUncompressedNodeSet(arr, buf));
        }
        else if (node_set_type == 2) {
          // Compressed group,
          message_str.addNodeSet(new CompressedNodeSet(arr, buf));
        }
        else {
          throw new RuntimeException("Unknown node set type: " + node_set_type);
        }
      }

      else if (type == 18) {
        long high_v = din.readLong();
        long low_v = din.readLong();
        message_str.addBlockId(new BlockId(high_v, low_v));
      }
      else if (type == 19) {
        int sz = din.readInt();
        BlockId[] arr = new BlockId[sz];
        for (int n = 0; n < sz; ++n) {
          long high_v = din.readLong();
          long low_v = din.readLong();
          arr[n] = new BlockId(high_v, low_v);
        }
        message_str.addBlockIdArr(arr);
      }

      else if (type == 20) {
        message_str.addPathInfo(readPathInfo(din));
      }
      else if (type == 21) {
        int sz = din.readInt();
        PathInfo[] arr = new PathInfo[sz];
        for (int n = 0; n < sz; ++n) {
          arr[n] = readPathInfo(din);
        }
        message_str.addPathInfoArr(arr);
      }


      else {
        throw new RuntimeException("Unknown message type on stream " + type);
      }
    }
    // Consume the last byte type,
    byte v = din.readByte();
    if (v != 8) {
      throw new RuntimeException("Expected '8' to end message stream");
    }
    // Return the message str
    return message_str;
  }


  /**
   * For debugging purposes.
   */
  @Override
  public String toString() {
    return commands.toString();
  }


  private static String MESSAGE_CLOSE = new String("]");

  // ----- Inner classes -----

  private static class StringArg {
    String str_ob;
    StringArg(String v) {
      this.str_ob = v;
    }
  }

  private static class MSMessage implements Message {

    private String name;
    private ArrayList args;

    MSMessage(String name) {
      this.name = name;
      this.args = new ArrayList(8);
    }

    public String getName() {
      return name;
    }

    public int count() {
      return args.size();
    }

    public Object param(int n) {
      return args.get(n);
    }

    public boolean isError() {
      return name.equals("E");
    }
    
    public String getErrorMessage() {
      ExternalThrowable e = (ExternalThrowable) args.get(0);
      return e.getMessage();
    }

    public ExternalThrowable getExternalThrowable() {
      return (ExternalThrowable) args.get(0);
    }
    
  }

  private class MessageIterator implements Iterator<Message> {

    private int pos = 0;

    public MessageIterator() {
    }

    public boolean hasNext() {
      return pos < commands.size();
    }

    public Message next() {
      String msg_name = (String) commands.get(pos);
      MSMessage msg = new MSMessage(msg_name);
      ++pos;
      while (true) {
        Object v = commands.get(pos);
        ++pos;
        if (v == null) {
          msg.args.add(v);
        }
        else if (v instanceof String && v.equals(MESSAGE_CLOSE)) {
          return msg;
        }
        else if (v instanceof StringArg) {
          msg.args.add(((StringArg) v).str_ob);
        }
        else {
          msg.args.add(v);
        }
      }
    }

    public void remove() {
      throw new RuntimeException("Remove not supported");
    }

  }

}
