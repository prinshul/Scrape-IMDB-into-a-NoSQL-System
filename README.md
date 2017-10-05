# Scrape-IMDB-into-a-NoSQL-System
Writing a program or script to populate a NoSQL database with the information stored in RDBMS.
For instance you could read the data in your MySQL database and populate a MongoDB database with documents corresponding to movies (or actors, or both, etc.).
Here you are asked to come up with your own program (in Java or Python) that will
use a database connector (e.g., JDBC for Java) to collect the data on one side and write it in the other side.
1. use a database connector (e.g., JDBC for Java or MySQL Connector for Python etc.)
to read the information from your local IMDB database in your application
2. design document(s) for your document database (if, for instance, you use MongoDB)
this can be done with any type of NoSQL database management system.
3. populate the NoSQL database (e.g., MongoDB) using your program (e.g., https://docs.mongodb.com/ecosystem/drivers/java/ for Java)
4. write some tests that verify that the transfer is correct: dene some test scenarios/use cases and write some unit tests to check that a certain number of facts are equivalent in both databases (e.g., MySQL and MongoDB).
Note that the structure of the NoSQL database has an impact on the performance of the queries you will be sending to the NoSQL database management system.
