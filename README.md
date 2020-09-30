**JDBC CRUD Operations**

* Select
* Insert
* Update
* Delete
* Bulk Insert

Main project is a SpringBoot application  - additional implementation can be added to link api
links to CRUD operations so that they can be triggered from browser.

Bulk insert was implemented through 2 different approaches :
 1. PreparedStatement
 2. Using standard insert "Insert INTO table VALUES(v1, v2, ...,vn),(v1, v2, ...,vn);", 
 with providing multiple INSERT INTO when rows number exceeds 1000 ( known java limitation);
 
 Implemented methods can be tested on a BikeStores database : 
 https://www.sqlservertutorial.net/sql-server-sample-database/
 
 Prerequisites : 
 1. MSSQL Server Express - installed and sa account with password created
 2. Any Java IDE - preferably IntelliJ Idea
 3. Should work on any java distribution starting > 1.8

Execution : 
Can be executed by running separate test methods from src/test/java/com/example/demo/JDBCTest.java

 