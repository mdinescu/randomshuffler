using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;

using System.IO.MemoryMappedFiles;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   
///    RandomShuffler is a .NET 4 tool that can generate & shuffle a list of
///                   32bit numbers, up to a specified maximum (up to 2^32)
///    
///    Author:        M. Dinescu    <mdinescu@donaq.com>
///    
///    DESCRIPTION:
///    The tool can also upload the numbers from the list to a table in a SQL 
///    database, using the bulk copy mechanism to speed up the upload.
///    
///    The tool is released as open source software and as such there are no
///    express or implied warranty claims.
///    
///    All interaction happens via an interactive console.
///    
///    By default, the list is generated in a file named: numbers.lst
///    
///    The table schema is:    CodeIndex (INT64, Code INT32, CodeType BIT, Assigned INT32)
///    
///    The CodeIndex is a numeric index that is incremented for each value added to the list.
///    
///    The Code is the the actual randomized numbers
///    
///    The CodeType will be set to 0 for numbers less than 2^31, and 1 otherwise.
///    
///    The Assigned field will alsways be null (to be used later)
///
/////////////////////////////////////////////////////////////////////////////////////////////////////////////
namespace DNQ.RandomSuffler
{
    class Program
    {
        static uint maxNum = 0;
        static string serverName = ".";
        static string databaseName = "";
        static string username = "sa";
        static string password = "";
        static string tableName = "";
        static string inputFile = "numbers.lst";
        static System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

        static void GenerateList()
        {
            Console.Clear();

            Console.WriteLine("Random List Generator\r\n==============================================================================");
            Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
            Console.WriteLine();
            Console.WriteLine("[List Generator]");
            Console.WriteLine();
            do
            {
                Console.SetCursorPosition(0, 6);
                Console.WriteLine("                                                                                                   ");
                Console.SetCursorPosition(0, 5);
                Console.WriteLine("Enter # of values to generate 1 - [4294967296]: ");
                string val = Console.ReadLine();
                if (val == "")
                    maxNum = UInt32.MaxValue;
                else
                    UInt32.TryParse(val, out maxNum);
            } while (maxNum == 0);

            if (maxNum > 100000000)
            {
                Console.SetCursorPosition(0, 6);
                Console.WriteLine("                                                                                                      ");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Warning! The list will require approx. {0:0.000}GB of disk space..    \r\n[Press Any Key To Continue or ESC to cancel]", ((double)maxNum / 250000000));
                Console.ForegroundColor = ConsoleColor.Gray;
                if (Console.ReadKey().Key == ConsoleKey.Escape)
                    return;
            }
            
            Console.SetCursorPosition(0, 6);
            Console.WriteLine("                                                                                                      ");
            Console.WriteLine("Shuffling a list of {0} integers                                                         ", maxNum);

            sw.Start();
            using (var memMap = MemoryMappedFile.CreateFromFile(inputFile, System.IO.FileMode.Create, "ListMap", (long)maxNum * 4, MemoryMappedFileAccess.ReadWrite))
            {
                using (var memView = memMap.CreateViewAccessor())
                {
                    sw.Restart();

                    for (uint idx = 0; idx < maxNum; idx++)
                    {
                        memView.Write((long)idx * 4, idx);
                    }

                    Console.WriteLine("Generated initial list. {0}", sw.Elapsed);

                    Console.WriteLine("Shuffling..");
                    var rng = System.Security.Cryptography.RNGCryptoServiceProvider.Create();
                    int milCnt = 0;
                    for (uint idx = maxNum - 1; idx > 0; idx--)
                    {
                        byte[] b = new byte[4];
                        rng.GetBytes(b);
                        uint rndNumber = BitConverter.ToUInt32(b, 0);
                        uint rndPosition = (uint)(rndNumber % idx);

                        long srcPos = (long)idx * 4;
                        long destPos = (long)rndPosition * 4;

                        uint tmp = memView.ReadUInt32(srcPos);
                        memView.Write(srcPos, memView.ReadUInt32(destPos));
                        memView.Write(destPos, tmp);

                        if (idx % 1000000 == 0)
                        {
                            milCnt++;
                            Console.SetCursorPosition(0, 11);
                            Console.WriteLine("{0} million numbers shuffled -> time elapsed {1}", milCnt, sw.Elapsed);
                        }
                    }
                    Console.SetCursorPosition(0, 11);
                    Console.WriteLine("{0} million numbers shuffled -> time elapsed {1}", milCnt, sw.Elapsed);
                    sw.Stop();
                }
            }

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Done.. Total time elapsed {0}                      [Press Any Key]", sw.Elapsed);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.ReadKey();
        }

        static void UploadToServer()
        {
            do
            {
                Console.Clear();

                Console.WriteLine("Random List Generator\r\n==============================================================================");
                Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
                Console.WriteLine();
                Console.WriteLine("[Upload to Server]");
                Console.WriteLine();
  
                do
                {
                    Console.Clear();

                    Console.WriteLine("Random List Generator\r\n==============================================================================");
                    Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
                    Console.WriteLine();
                    Console.WriteLine("[Upload to Server]");
                    Console.WriteLine();

                    Console.WriteLine("Please select:");
                    Console.WriteLine("1. Create table");
                    Console.WriteLine("2. Drop table");
                    Console.WriteLine("3. Upload numbers");
                    Console.WriteLine("4. Change configuration");
                    Console.WriteLine("0. Return to main menu");

                    var key = Console.ReadKey();
                    if (key.Key == ConsoleKey.Escape || key.Key == ConsoleKey.D0) return;
                    if (key.Key == ConsoleKey.D1) CreateTable();
                    if (key.Key == ConsoleKey.D2) DropTable();
                    if (key.Key == ConsoleKey.D3) UploadNumbers();
                    if (key.Key == ConsoleKey.D4) ConfigUpload();
                } while (true);
            } while (true);

            Console.Write("\r\n[Press Any Key]                                                                                        ");
            Console.ReadKey();
        }

        static void ConfigUpload()
        {
            do
            {
                Console.Clear();

                Console.WriteLine("Random List Generator\r\n==============================================================================");
                Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
                Console.WriteLine();
                Console.WriteLine("[Upload to Server]");
                Console.WriteLine("");


                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;


                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter server name [" + serverName + "]:                ");

                    var val = Console.ReadLine();
                    if (val != "") serverName = val;
                } while (serverName == "");
                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:    "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter database name [" + databaseName + "]:              ");
                    var val = Console.ReadLine();
                    if (val != "") databaseName = val;
                } while (databaseName == "");
                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter username [" + username + "]:                ");

                    var val = Console.ReadLine();
                    if (val != "") username = val;
                } while (username == "");
                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter password [***********]:                 ");
                    var val = Console.ReadLine();
                    if (val != "") password = val;
                } while (password == "");
                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter table name [" + tableName + "]:                ");

                    var val = Console.ReadLine();
                    if (val != "") tableName = val;
                } while (tableName == "");
                do
                {
                    Console.SetCursorPosition(0, 5);

                    Console.WriteLine("\r\nCurrent Settings:                                                                              ");
                    Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                    Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                    Console.SetCursorPosition(0, 15);
                    Console.WriteLine("                                                                                                   ");
                    Console.SetCursorPosition(0, 14);
                    Console.WriteLine("Enter input file [" + inputFile + "]:                ");

                    var val = Console.ReadLine();
                    if (val != "") inputFile = val;

                    if (!System.IO.File.Exists(inputFile))
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.SetCursorPosition(0, 15);
                        Console.WriteLine(" ERROR: " + inputFile + " does not exist!");
                        Console.ForegroundColor = ConsoleColor.Gray;
                        inputFile = "";

                        Console.ReadKey();
                    }

                } while (inputFile == "");

                Console.SetCursorPosition(0, 5);
                Console.WriteLine("\r\nCurrent Settings:                                                                                   ");
                Console.Write(" Server:     "); Console.ForegroundColor = ConsoleColor.White; Console.Write(serverName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write(" Database:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(databaseName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write(" Username:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write(username + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write(" Password:   "); Console.ForegroundColor = ConsoleColor.White; Console.Write("***********\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write(" Table:      "); Console.ForegroundColor = ConsoleColor.White; Console.Write(tableName + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;
                Console.Write(" Input File: "); Console.ForegroundColor = ConsoleColor.White; Console.Write(inputFile + "\r\n"); Console.ForegroundColor = ConsoleColor.Gray;

                Console.SetCursorPosition(0, 14);
                Console.WriteLine("                                                                                                   ");
                Console.SetCursorPosition(0, 15);
                Console.WriteLine("                                                                                                   ");
                Console.SetCursorPosition(0, 15);
                Console.Write("Are these values correct [Y]/n: ");

                var key = Console.ReadKey();
                if (key.Key == ConsoleKey.Enter || key.Key == ConsoleKey.Y) break;
                if (key.Key == ConsoleKey.Escape) return;
            } while (true);
        }

        static void CreateTable()
        {
            Console.Clear();

            Console.WriteLine("Random List Generator\r\n==============================================================================");
            Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
            Console.WriteLine();
            Console.WriteLine("[Upload to Server]");
            Console.WriteLine("   - Create Table -");

            if (databaseName == "" || password == "" || tableName == "")
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\r\nInvalid configuration! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }

            Console.SetCursorPosition(0, 5);
            Console.Write(" Will create table ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(tableName);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" in database ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(databaseName);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" on ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(serverName);
            Console.ForegroundColor = ConsoleColor.Gray;

            Console.WriteLine("\r\nPress any key to begin..");
            Console.ReadKey();

            SqlConnectionStringBuilder scBuilder = new SqlConnectionStringBuilder();
            scBuilder.ApplicationName = "ListGenerator";
            scBuilder.DataSource = serverName;
            scBuilder.InitialCatalog = databaseName;
            scBuilder.UserID = username;
            scBuilder.Password = password;
            scBuilder.ConnectTimeout = 5;

            try
            {
                using (var sqCon = new SqlConnection(scBuilder.ToString()))
                {
                    sqCon.Open();

                    using (var sqCmd = sqCon.CreateCommand())
                    {
                        sqCmd.CommandType = CommandType.Text;
                        sqCmd.CommandText = "CREATE TABLE " + tableName + " (CodeIndex INTEGER PRIMARY KEY, Code INTEGER NOT NULL, CodeType TINYINT, Assigned INTEGER NULL)";                        

                        sqCmd.ExecuteNonQuery();

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("\r\nSUCCESS! The table " + tableName + " was created successfully");
                        Console.ForegroundColor = ConsoleColor.Gray;
                        Console.ReadKey();
                    }
                }
            }
            catch (Exception exc)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\r\n" + exc.Message);
                Console.WriteLine("\r\nError creating table! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }
        }

        static void DropTable()
        {
            Console.Clear();

            Console.WriteLine("Random List Generator\r\n==============================================================================");
            Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
            Console.WriteLine();
            Console.WriteLine("[Upload to Server]");
            Console.WriteLine("   - Drop Table -");

            if (databaseName == "" || password == "" || tableName == "")
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\r\nInvalid configuration! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }

            Console.SetCursorPosition(0, 5);
            Console.Write(" Will drop table ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(tableName);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" from database ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(databaseName);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" on ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(serverName);
            Console.ForegroundColor = ConsoleColor.Gray;

            Console.WriteLine("\r\nPress any key to begin..");
            Console.ReadKey();

            SqlConnectionStringBuilder scBuilder = new SqlConnectionStringBuilder();
            scBuilder.ApplicationName = "ListGenerator";
            scBuilder.DataSource = serverName;
            scBuilder.InitialCatalog = databaseName;
            scBuilder.UserID = username;
            scBuilder.Password = password;
            scBuilder.ConnectTimeout = 5;

            try
            {
                using (var sqCon = new SqlConnection(scBuilder.ToString()))
                {
                    sqCon.Open();

                    using (var sqCmd = sqCon.CreateCommand())
                    {
                        sqCmd.CommandType = CommandType.Text;
                        sqCmd.CommandText = "DROP TABLE " + tableName;

                        sqCmd.ExecuteNonQuery();

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine("\r\nSUCCESS! The table " + tableName + " was created successfully");
                        Console.ForegroundColor = ConsoleColor.Gray;
                        Console.ReadKey();
                    }
                }
            }
            catch (Exception exc)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\r\n" + exc.Message);
                Console.WriteLine("\r\nError dropping table! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }
        }

        
        /// <summary>
        /// Interactively uploads numbers from the shuffled list into a SQL database table.
        /// 
        /// The database and the table must exist (see CreateTable for a helper method that creates the table)
        /// 
        /// </summary>
        static void UploadNumbers()
        {
            Console.Clear();

            Console.WriteLine("Random List Generator\r\n==============================================================================");
            Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
            Console.WriteLine();
            Console.WriteLine("[Upload to Server]");
            Console.WriteLine("   - Upload Numbers (" + maxNum + ")-");

            if (databaseName == "" || password == "" || tableName == "" || !System.IO.File.Exists(inputFile))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("\r\nInvalid configuration! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }

            do
            {
                Console.SetCursorPosition(0, 5);
                Console.Write("                                                                                                       ");
                Console.SetCursorPosition(0, 5);

                Console.Write("How many numbers to upload (" + maxNum + ")? ");
                Console.ForegroundColor = ConsoleColor.White; 
                var val = Console.ReadLine();
                Console.ForegroundColor = ConsoleColor.Gray;

                if (val != "")
                    UInt32.TryParse(val, out maxNum);
            } while (maxNum == 0);

            Console.SetCursorPosition(0, 5);
            Console.Write(" Will upload ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(maxNum);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" numbers into database ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(databaseName);
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.Write(" on ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(serverName);
            Console.ForegroundColor = ConsoleColor.Gray;

            Console.WriteLine("\r\nPress any key to begin..");
            Console.ReadKey();

            SqlConnectionStringBuilder scBuilder = new SqlConnectionStringBuilder();
            scBuilder.ApplicationName = "ListGenerator";
            scBuilder.DataSource = serverName;
            scBuilder.InitialCatalog = databaseName;
            scBuilder.UserID = username;
            scBuilder.Password = password;
            scBuilder.ConnectTimeout = 5;

            try
            {
                using (var sqCon = new SqlConnection(scBuilder.ToString()))
                {
                    sqCon.Open();

                    using (var sqCmd = sqCon.CreateCommand())
                    {
                        sqCmd.CommandType = CommandType.Text;
                        sqCmd.CommandText = "SELECT CodeIndex,Code,CodeType,Assigned FROM " + tableName;

                        sqCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception exc)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + exc.Message);
                Console.WriteLine("\r\nError connecting to database! Please go back and update configuration..");
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
                return;
            }


            sw.Restart();
            try
            {
                using (var bc = new SqlBulkCopy(scBuilder.ToString()))
                {
                    bc.BatchSize = 100000;
                    bc.BulkCopyTimeout = 20;
                    bc.DestinationTableName = tableName;
                    bc.NotifyAfter = 100000;
                    bc.SqlRowsCopied += bc_SqlRowsCopied;

                    using (var listMapper = new ListDataMapper(inputFile))
                    {
                        bc.WriteToServer(listMapper);
                    }
                }
            }
            catch (Exception exc)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: " + exc.Message);
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.ReadKey();
            }
            sw.Stop();

            Console.WriteLine("\r\nPress any key to return to upload menu.");
            Console.ReadKey();
        }

        /// <summary>
        /// notifies on the console as records are uploaded to the SQL database via bulk copy
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        static void bc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.SetCursorPosition(0, 15);
            Console.Write(" Copied {0} records in {1}", e.RowsCopied, sw.Elapsed);
        }


        /// <summary>
        /// Application entry point - displays an interactive menu 
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)        
        {
            do
            {
                Console.Clear();

                Console.WriteLine("Random List Generator\r\n==============================================================================");
                Console.WriteLine("    M. Dinescu <mdinescu@donaq.com>");
                Console.WriteLine();

                Console.WriteLine("Please Select:");
                Console.WriteLine("1. Generate New List");
                Console.WriteLine("2. Upload List to Database");
                Console.WriteLine("0. Exit [ESC]");

                var key = Console.ReadKey();
                if (key.Key == ConsoleKey.Escape || key.Key == ConsoleKey.D0) break;
                if (key.Key == ConsoleKey.D1) GenerateList();
                if (key.Key == ConsoleKey.D2) UploadToServer();
            } while (true);
        }
    }
}
