/**
 * com.mckoi.runtime.PathDumpTool  Jul 5, 2010
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

package com.mckoi.runtime;

import com.mckoi.data.DataFile;
import com.mckoi.data.Key;
import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.TreeSystemTransaction;
import com.mckoi.network.CommitFaultException;
import com.mckoi.network.DataAddress;
import com.mckoi.network.MckoiDDBClient;
import com.mckoi.network.MckoiDDBClientUtils;
import com.mckoi.sdb.SDBTransaction;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * A simple command line tool that dumps the entire content of a path out
 * to a file in a simple binary format. The 'export' function works across all
 * versions of MckoiDDB.
 * <p>
 * This is intended as a simple way to export data from an existing database
 * and import it into a new format. It is not intended to be used as a backup
 * facility. For backups, use a block level tool.
 *
 * @author Tobias Downer
 */

public class PathDumpTool {

  private static void exportDump(String[] args) {
    String path_name = args[1];

    try {
      MckoiDDBClient client =
                      MckoiDDBClientUtils.connectTCP(new File("client.conf"));

      // Get the current transaction,
      DataAddress snapshot = client.getCurrentSnapshot(path_name);
      KeyObjectTransaction transaction = client.createTransaction(snapshot);

      // Cast it to a tree system transaction,
      TreeSystemTransaction system_t = (TreeSystemTransaction) transaction;

      String consensus_function;
      try {
        consensus_function = client.getConsensusFunction(path_name);
      }
      catch (Throwable e) {
        // If this function isn't available, get from the command line
        consensus_function = args[2];
      }

      File path_file = new File(path_name + ".mdump");

      FileOutputStream fout = new FileOutputStream(path_file);
      BufferedOutputStream bout = new BufferedOutputStream(fout);
      DataOutputStream dout = new DataOutputStream(bout);

      dout.writeUTF(path_name);
      dout.writeUTF(consensus_function);

      byte[] buf = new byte[1024];

      Iterator<Key> key_iterator = system_t.allKeys();
      while (key_iterator.hasNext()) {
        Key key = key_iterator.next();

        // Get and write the key values,
        long key0 = key.encodedValue(1);
        long key1 = key.encodedValue(2);
        dout.writeLong(key0);
        dout.writeLong(key1);

        // Write the data content,
        DataFile df = system_t.getDataFile(key, 'r');
        long sz = df.size();

        dout.writeLong(sz);

        long c = sz;
        while (c > 0) {
          int to_write = (int) Math.min(buf.length, c);
          if (to_write > 0) {
            df.get(buf, 0, to_write);
            dout.write(buf, 0, to_write);
          }
          c -= to_write;
        }
      }

      dout.flush();
      dout.close();

    }
    catch (IOException e) {
      System.out.println("Error: " + e.getMessage());
    }

  }

  private static void importDump(String[] args) {
    String dump_filename = args[1];

    try {
      MckoiDDBClient client =
                      MckoiDDBClientUtils.connectTCP(new File("client.conf"));

      if (!dump_filename.endsWith(".mdump")) {
        dump_filename = dump_filename + ".mdump";
      }

      // The dump file,
      File dump_file = new File(dump_filename);

      if (!dump_file.exists()) {
        System.out.println("File " + dump_file.toString() + " doesn't exist");
        return;
      }

      // Translate it into a path name,
      String path_name = dump_file.getName();
      path_name = path_name.substring(0, path_name.length() - 6);

      FileInputStream fin = new FileInputStream(dump_file);
      BufferedInputStream bin = new BufferedInputStream(fin);
      DataInputStream din = new DataInputStream(bin);

      // Read the path name and consensus function,
      String in_path_name = din.readUTF();
      String in_consensus_function = din.readUTF();

      // Get the current transaction,
      DataAddress snapshot = client.getCurrentSnapshot(in_path_name);
      KeyObjectTransaction transaction = client.createTransaction(snapshot);

      // Cast it to a tree system transaction,
      TreeSystemTransaction system_t = (TreeSystemTransaction) transaction;

      String consensus_function = null;
      try {
        consensus_function = client.getConsensusFunction(path_name);
      }
      catch (Throwable e) {
      }

      // Compare consensus,
      if (!consensus_function.equals(in_consensus_function)) {
        System.out.println("Unable to import, consensus functions do not match.");
        return;
      }

      byte[] buf = new byte[1024];

      try {
        while (true) {
          long key0 = din.readLong();
          long key1 = din.readLong();

          short type = (short) (key0 >> 32);
          int secondary_key = (int) (key0 & 0x0FFFFFFFF);
          long primary_key = key1;

          Key key = new Key(type, secondary_key, primary_key);

          long sz = din.readLong();
          DataFile df = system_t.getDataFile(key, 'w');

          long c = sz;
          while (c > 0) {
            int to_read = (int) Math.min(buf.length, c);
            if (to_read > 0) {
              din.readFully(buf, 0, to_read);
              df.put(buf, 0, to_read);
            }
            c -= to_read;
          }

          System.out.print(".");

        }
      }
      catch (EOFException e) {
        // End reached, so complete,
      }

      if (consensus_function.equals("com.mckoi.sdb.SimpleDatabase")) {
        SDBTransaction.writeForcedTransactionIntroduction(transaction);
      }
      else {
        // Don't know how to introduce data of this type,
        System.out.println("ERROR: Do not know how to introduce a snapshot to");
        System.out.println("  path type: " + consensus_function);
        return;
      }

      DataAddress flushed_snapshot = client.flushTransaction(transaction);
      client.performCommit(in_path_name, flushed_snapshot);

    }
    catch (IOException e) {
      System.out.println("Error: " + e.getMessage());
    }
    catch (CommitFaultException e) {
      System.out.println("Commit fault: " + e.getMessage());
    }

  }

  public static void main(String[] args) {

    if (args.length <= 1) {
      System.out.println("Syntax");
      System.out.println();
      System.out.println("PathDumpTool export [path name]");
      System.out.println("PathDumpTool import [.mdump file name]");
      System.out.println();
      return;
    }

    String command = args[0];

    if (command.equals("export")) {
      exportDump(args);
    }
    else if (command.equals("import")) {
      importDump(args);
    }
    else {
      System.out.println("Unknown command: " + command);
    }

  }

}
