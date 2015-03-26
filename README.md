# Random Shuffler

A generator for lists of random numbers (shuffled lists of numbers)

DESCRIPTION:
=============

RandomShuffler is a .NET 4 tool that can generate & shuffle a list of 32bit numbers, up to a specified maximum (currently the maximum is limited up to 2^32)

The tool can also upload the numbers from the list to a table in a SQL database, using the bulk copy mechanism to speed up the upload.

The tool is released as open source software and as such there are no express or implied warranty claims.

All interaction happens via an interactive console.
    
By default, the list is generated in a file named: numbers.lst
    
The table schema is:    

    CodeIndex INTEGER PRIMARY KEY, 
    Code INTEGER NOT NULL, 
    CodeType TINYINT NULL, 
    Assigned INTEGER NULL
    
The **CodeIndex** is a numeric index that is incremented for each value added to the list.
    
The **Code** is the the actual randomized numbers
    
The **CodeType** will be set to 0 for numbers less than 2^31, and 1 otherwise.
    
The **Assigned** field will alsways be null (to be used later)
