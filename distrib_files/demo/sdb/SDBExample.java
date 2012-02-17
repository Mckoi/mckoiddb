/**
 * SDBExample  Aug 14, 2009
 */

import com.mckoi.network.*;
import com.mckoi.sdb.*;
import com.mckoi.data.*;
import java.io.File;
import java.io.IOException;

/**
 * A simple example of using the MckoiDDB client API to connect
 * to a network installation and perform some operations. This
 * example looks for a 'client.conf' file in the current
 * directory. The 'client.conf' file contains information about
 * how to connect to the network installation.
 *
 * To make this demo work you will need to edit 'client.conf' and
 * set it appropriately for your installation. You will also
 * need a Simple Database path instance called 'testdb'
 * set up on your installation.
 */

public class SDBExample {

  public static void main(String[] args) {
    // The 'client.conf' file,
    File client_file = new File("client.conf");

    // Connect to the network,
    MckoiDDBClient client;
    try {
      client = MckoiDDBClientUtils.connectTCP(client_file);
    }
    catch (IOException e) {
      e.printStackTrace();
      return;
    }

    // Create an SDBSession object on our named path.
    SDBSession session = new SDBSession(client, "testdb");

    // Create a new transaction,
    SDBTransaction transaction = session.createTransaction();
    // Create the table 'BooksWithRobots'
    boolean created = transaction.createTable("BooksWithRobots");
    // If we created a new table then we populate it with data
    if (created) {
      SDBTable robot_books = transaction.getTable("BooksWithRobots");
      // Add the columns and indexes of the table,
      robot_books.addColumn("name");
      robot_books.addColumn("author");
      robot_books.addColumn("year");
      robot_books.addIndex("author");
      robot_books.addIndex("year");

      // Insert 4 rows,
      robot_books.insert();
      robot_books.setValue("name", "The Hitchhiker's Guide to the Galaxy");
      robot_books.setValue("author", "Douglas Adams");
      robot_books.setValue("year", "1979");
      robot_books.complete();
      robot_books.insert();
      robot_books.setValue("name", "I, Robot");
      robot_books.setValue("author", "Isaac Asimov");
      robot_books.setValue("year", "1950");
      robot_books.complete();
      robot_books.insert();
      robot_books.setValue("name", "2001 : A Space Odyssey");
      robot_books.setValue("author", "Authur C. Clarke");
      robot_books.setValue("year", "1968");
      robot_books.complete();
      robot_books.insert();
      robot_books.setValue("name", "The Rest of the Robots");
      robot_books.setValue("author", "Isaac Asimov");
      robot_books.setValue("year", "1964");
      robot_books.complete();
    }

    // Commit the changes,
    try {
      transaction.commit();
    }
    catch (CommitFaultException e) {
      // In this example, this could only happen if a
      // concurrent transaction also created a table called
      // 'BooksWithRobots'
      System.out.println("Oops, we failed to commit.");
      return;
    }

    // Now a simple query on the data,

    // Create a new transaction,
    transaction = session.createTransaction();
    // Fetch the 'BooksWithRobots' table,
    SDBTable robot_books = transaction.getTable("BooksWithRobots");
    // Fetch the author index,
    SDBIndex index = robot_books.getIndex("author");
    // Find the sub-index of all entries that are 'Isaac Asimov' from the
    // author index.
    index = index.sub("Isaac Asimov", true, "Isaac Asimov", true);
    // Print the name and year of the books,
    for (SDBRow row : index) {
      System.out.print(row.getValue("name"));
      System.out.print(" (");
      System.out.print(row.getValue("year"));
      System.out.println(")");
    }

  }
}

