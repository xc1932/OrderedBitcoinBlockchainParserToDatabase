using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using BitcoinBlockchain.Data;
using BitcoinBlockchain.Parser;
using OrderedBitcoinBlockchainParser;
using NBitcoin;
using BitcoinUTXOSlicer;
using System.Diagnostics;

namespace OrderedBitcoinBlockchainParserToDatabase
{
    public class OrderedBitcoinBlockchainParserToDatabase_Class
    {
        public string sqlConnectionString = null;
        BitcoinUTXOSlicer_Class bitcoinUTXOSlicer;
        public Int64 nextBlockID;
        public Int64 nextTransactionID;
        public Int64 nextTxInputID;
        public Int64 nextTxOutputID;
        public Int64 nextOpreturnOutputID;

        public OrderedBitcoinBlockchainParserToDatabase_Class() { }

        public OrderedBitcoinBlockchainParserToDatabase_Class(string blockchainFilePath, string blockProcessContextFilePath, string blockProcessContextFileName, string UtxoSliceFilePath,
            string UtxoSliceFileName, string OpReturnFilePath, string sliceIntervalTimeType, int sliceIntervalTime, DateTime endTime, int endBlockHeight, string sqlConnectionString)
        {
            this.sqlConnectionString = sqlConnectionString;
            initialization_Database(true);
            bitcoinUTXOSlicer = new BitcoinUTXOSlicer_Class(blockchainFilePath, blockProcessContextFilePath, blockProcessContextFileName, UtxoSliceFilePath,
            UtxoSliceFileName, OpReturnFilePath, sliceIntervalTimeType, sliceIntervalTime, endTime, endBlockHeight, sqlConnectionString);
            restore_DatabaseForBlockParserTable();
        }

        ////I.*****数据库初始化*****
        //----1.测试数据库连接状态----
        public bool databaseConnectTest(bool printMark)
        {
            bool result = false;
            if (sqlConnectionString != null)
            {
                using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                {
                    try
                    {
                        sqlConnection.Open();
                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            if (printMark)
                            {
                                Console.WriteLine("数据库连接成功!!!");
                            }
                            result = true;
                        }
                        else
                        {
                            if (printMark)
                            {
                                Console.WriteLine("数据库连接失败!!!");
                            }
                        }
                    }
                    catch
                    {
                        if (printMark)
                        {
                            Console.WriteLine("数据库连接失败!!!");
                        }
                    }
                    finally
                    {
                        sqlConnection.Close();
                    }
                }
            }
            else
            {
                if (printMark)
                {
                    Console.WriteLine("数据库连接字符串不能为空!!!");
                }
            }
            return result;
        }

        //----2.数据库表、存储过程和表值是否存在----
        //a.判断数据库中是否存在某张表
        public bool tableExist(string tableName)
        {
            bool tableExist = false;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                    using (DataTable dt = sqlConnection.GetSchema("Tables"))
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (string.Equals(tableName, dr[2].ToString()))
                            {
                                tableExist = true;
                                break;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw e;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
            return tableExist;
        }

        //b.判断数据库中是否存在某个存储过程
        public bool procedureExist(string procedureName)
        {
            bool procedureExist = false;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                    using (DataTable dt = sqlConnection.GetSchema("Procedures"))
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (string.Equals(procedureName, dr[2].ToString()))
                            {
                                procedureExist = true;
                                break;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw e;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
            return procedureExist;
        }

        //c.判断数据库中是否存在某个存储过程
        public bool tableTypeExist(string tableTypeName)
        {
            bool tableTypeExist = false;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                    using (DataTable dt = sqlConnection.GetSchema("DataTypes"))
                    {
                        foreach (DataRow dr in dt.Rows)
                        {
                            if (string.Equals(tableTypeName, dr[0].ToString()))
                            {
                                tableTypeExist = true;
                                break;
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw e;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
            return tableTypeExist;
        }

        //----3.创建数据库表、存储程序和表值----
        //a.分别判断关于区块的5张表是否存在，没有则创建
        public void blockTableCreate(bool printMark)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand;
                //1.创建ProcessedBlockID表
                if (!tableExist("ProcessedBlockID"))
                {
                    string commandStr = "CREATE TABLE [dbo].[ProcessedBlockID]([BlockID] [bigint] NOT NULL DEFAULT (-1))";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("ProcessedBlockID表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("ProcessedBlockID表已存在!!!");
                    }
                }
                //2.创建Block表
                if (!tableExist("Block"))
                {
                    string commandStr = "CREATE TABLE [dbo].[Block]([BlockID][bigint] NOT NULL PRIMARY KEY,[BlockHeight] [bigint] NOT NULL," +
                        "[BlockHash] [binary](32) NOT NULL,[PreviousBlockHash] [binary](32) NOT NULL,[BlockTimestamp] [datetime] NOT NULL," +
                        "[BlockVersion] [int] NOT NULL,[BlockSize] [bigint] NOT NULL,[Bits] [binary](4) NOT NULL,[Nonce] [binary](4) NOT NULL," +
                        "[TxCount] [bigint] NOT NULL,[HashMerkleRoot] [binary](32) NOT NULL,[BlockchainFileId] [int] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("Block表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("Block表已存在!!!");
                    }
                }
                //3.创建Transaction表
                if (!tableExist("Transaction"))
                {
                    string commandStr = "CREATE TABLE [dbo].[Transaction]([TxID] [bigint] NOT NULL PRIMARY KEY,[TxHash] [binary](32) NOT NULL," +
                        "[BlockID] [bigint] NOT NULL,[TxVersion] [int] NOT NULL,[TxInCount] [bigint] NOT NULL,[TxOutCount][bigint] NOT NULL," +
                        "[LockTime] [int] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("Transaction表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("Transaction表已存在!!!");
                    }
                }
                //4.创建TransactionInput表
                if (!tableExist("TransactionInput"))
                {
                    string commandStr = "CREATE TABLE [dbo].[TransactionInput]([TxInID][bigint] NOT NULL PRIMARY KEY,[TxID] [bigint] NOT NULL," +
                        "[SourceTxOutID] [bigint] NOT NULL,[Sequence] [bigint] NOT NULL,[InputScript] [varbinary](max)NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("TransactionInput表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("TransactionInput表已存在!!!");
                    }
                }
                //5.创建TransactionOutput表
                if (!tableExist("TransactionOutput"))
                {
                    string commandStr = "CREATE TABLE [dbo].[TransactionOutput]([TxOutID][bigint] NOT NULL PRIMARY KEY,[TxID] [bigint] NOT NULL," +
                        "[OutputIndex] [int] NOT NULL,[OutputValue] [bigint] NOT NULL,[OutputScript] [varbinary](max)NOT NULL,[AddressID] [bigint])";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("TransactionOutput表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("TransactionOutput表已存在!!!");
                    }
                }
                //6.创建OpreturnOutput表
                if (!tableExist("OpreturnOutput"))
                {
                    string commandStr = "CREATE TABLE [dbo].[OpreturnOutput]([OpreturnOutID][bigint] NOT NULL PRIMARY KEY,[TxID] [bigint] NOT NULL," +
                        "[OutputIndex] [int] NOT NULL,[OutputValue] [bigint] NOT NULL,[OutputScript] [varbinary](max)NOT NULL,[AddressID] [bigint])";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("OpreturnOutput表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("OpreturnOutput表已存在!!!");
                    }
                }
                //7.创建Address表
                if (!tableExist("Address"))
                {
                    string commandStr = "CREATE TABLE [dbo].[Address]([AddressID][bigint] NOT NULL PRIMARY KEY,[Address] [varchar](64) NOT NULL UNIQUE," +
                        "[ClusterID] [bigint] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("Address表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("Address表已存在!!!");
                    }
                }                
                sqlConnection.Close();
            }
        }

        //b.分别判断关于区块的5张表的存储过程是否存在，没有则创建
        public void blockTableProcedureCreate(bool printMark)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand;
                //1.创建ProcessedBlockID表的存储过程
                if (!procedureExist("Update_ProcessedBlockID_Proc"))
                {
                    string commandStr = "CREATE PROC Update_ProcessedBlockID_Proc @BlockID bigint AS " +
                                        "BEGIN IF EXISTS(SELECT TOP(1) BlockID FROM[dbo].[ProcessedBlockID]) BEGIN Update[dbo].[ProcessedBlockID] " +
                                        "SET[BlockID] = @BlockID END ELSE BEGIN INSERT INTO[dbo].[ProcessedBlockID] VALUES(@BlockID) END END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Update_ProcessedBlockID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Update_ProcessedBlockID_Proc已存在!!!");
                    }
                }
                //2.创建Block表的存储过程
                if (!procedureExist("Insert_Block_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_Block_Proc @BlockID bigint,@BlockHeight bigint,@BlockHash binary(32),@PreviousBlockHash binary(32)," +
                                        "@BlockTimestamp datetime,@BlockVersion int,@BlockSize bigint,@Bits binary(4),@Nonce binary(4)," +
                                        "@TxCount bigint,@HashMerkleRoot binary(32),@BlockchainFileId int AS BEGIN INSERT INTO[dbo].[Block] VALUES" +
                                        "(@BlockID,@BlockHeight,@BlockHash,@PreviousBlockHash,@BlockTimestamp,@BlockVersion,@BlockSize,@Bits,@Nonce,@TxCount," +
                                        "@HashMerkleRoot,@BlockchainFileId) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Block_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Block_Proc已存在!!!");
                    }
                }
                //3.创建Transaction表的存储过程
                if (!procedureExist("Insert_Transaction_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_Transaction_Proc @TxID bigint,@TxHash binary(32),@BlockID bigint,@TxVersion int,@TxInCount bigint,@TxOutCount bigint," +
                                        "@LockTime int AS BEGIN INSERT INTO [dbo].[Transaction] VALUES(@TxID, @TxHash, @BlockID, @TxVersion, @TxInCount, @TxOutCount, @LockTime) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Transaction_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Transaction_Proc已存在!!!");
                    }
                }
                //4.创建TransactionInput表的存储过程
                if (!procedureExist("Insert_TransactionInput_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_TransactionInput_Proc @TxInID bigint,@TxID bigint,@SourceTxOutID bigint,@Sequence bigint,@InputScript varbinary(max)" +
                                        " AS BEGIN INSERT INTO[dbo].[TransactionInput] VALUES(@TxInID, @TxID, @SourceTxOutID, @Sequence, @InputScript) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_TransactionInput_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_TransactionInput_Proc已存在!!!");
                    }
                }
                //5.创建TransactionOutput表的存储过程
                if (!procedureExist("Insert_TransactionOutput_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_TransactionOutput_Proc @TxOutID bigint,@TxID bigint,@OutputIndex int,@OutputValue bigint,@OutputScript varbinary(max)," +
                                        "@AddressID bigint AS BEGIN INSERT INTO[dbo].[TransactionOutput] VALUES(@TxOutID, @TxID, @OutputIndex, @OutputValue, @OutputScript, @AddressID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_TransactionOutput_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_TransactionOutput_Proc已存在!!!");
                    }
                }
                //6.创建OpreturnOutput表的存储过程
                if (!procedureExist("Insert_OpreturnOutput_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_OpreturnOutput_Proc @OpreturnOutID bigint,@TxID bigint,@OutputIndex int,@OutputValue bigint,@OutputScript varbinary(max)," +
                                        "@AddressID bigint AS BEGIN INSERT INTO[dbo].[OpreturnOutput] VALUES(@OpreturnOutID, @TxID, @OutputIndex, @OutputValue, @OutputScript, @AddressID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_OpreturnOutput_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_OpreturnOutput_Proc已存在!!!");
                    }
                }
                //7.创建Address表的存储过程
                if (!procedureExist("Insert_Address_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_Address_Proc @AddressID bigint,@Address varchar(64),@ClusterID bigint" +
                                        " AS BEGIN INSERT INTO[dbo].[Address] VALUES(@AddressID, @Address, @ClusterID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Address_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_Address_Proc已存在!!!");
                    }
                }
                //8.创建获取Block表最大BlockID的存储过程
                if (!procedureExist("Get_MaxBlockID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxBlockID_Proc @BlockID bigint OUTPUT AS BEGIN SELECT @BlockID = MAX(BlockID) FROM[dbo].[Block] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxBlockID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxBlockID_Proc已存在!!!");
                    }
                }
                //9.创建获取Transaction表最大TxID的存储过程
                if (!procedureExist("Get_MaxTransactionID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxTransactionID_Proc @TxID bigint OUTPUT AS BEGIN SELECT @TxID = MAX(TxID) FROM[dbo].[Transaction] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionID_Proc已存在!!!");
                    }
                }
                //10.创建获取TransactionInput表最大TxInID的存储过程
                if (!procedureExist("Get_MaxTransactionInputID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxTransactionInputID_Proc @TxInID bigint OUTPUT AS BEGIN SELECT @TxInID = MAX(TxInID) FROM[dbo].[TransactionInput] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionInputID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionInputID_Proc已存在!!!");
                    }
                }
                //11.创建获取TransactionOutput表最大TxOutID的存储过程
                if (!procedureExist("Get_MaxTransactionOutputID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxTransactionOutputID_Proc @TxOutID bigint OUTPUT AS BEGIN SELECT @TxOutID = MAX(TxOutID) FROM[dbo].[TransactionOutput] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionOutputID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxTransactionOutputID_Proc已存在!!!");
                    }
                }
                //12.创建获取OpreturnOutput表最大OpreturnOutID的存储过程
                if (!procedureExist("Get_MaxOpreturnOutputID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxOpreturnOutputID_Proc @OpreturnOutID bigint OUTPUT AS BEGIN SELECT @OpreturnOutID = MAX(OpreturnOutID) FROM[dbo].[OpreturnOutput] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxOpreturnOutputID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxOpreturnOutputID_Proc已存在!!!");
                    }
                }
                //13.创建获取Address表最大AddressID的存储过程
                if (!procedureExist("Get_MaxAddressID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxAddressID_Proc @AddressID bigint OUTPUT AS BEGIN SELECT @AddressID = MAX(AddressID) FROM[dbo].[Address] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxAddressID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxAddressID_Proc已存在!!!");
                    }
                }
                //14.创建获取Address表最大AddressID的存储过程
                if (!procedureExist("Get_MaxProcessedBlockID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxProcessedBlockID_Proc @BlockID bigint OUTPUT AS BEGIN SELECT @BlockID = MAX(BlockID) FROM[dbo].[ProcessedBlockID] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxProcessedBlockID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxProcessedBlockID_Proc已存在!!!");
                    }
                }
                //15.创建恢复Block表的存储过程
                if (!procedureExist("Restore_BlockTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_BlockTable_Proc @BlockID bigint AS BEGIN DELETE FROM [dbo].[Block] WHERE BlockID>@BlockID END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_BlockTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_BlockTable_Proc已存在!!!");
                    }
                }
                //16.创建恢复Transaction表的存储过程
                if (!procedureExist("Restore_TransactionTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_TransactionTable_Proc @BlockID bigint AS BEGIN DELETE FROM [dbo].[Transaction] WHERE BlockID>@BlockID END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionTable_Proc已存在!!!");
                    }
                }
                //17.创建恢复TransactionInput表的存储过程
                if (!procedureExist("Restore_TransactionInputTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_TransactionInputTable_Proc @BlockID bigint AS BEGIN DELETE FROM [dbo].[TransactionInput] " +
                                        "WHERE TxID IN(SELECT TxID FROM [dbo].[Transaction] WHERE BlockID>@BlockID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionInputTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionInputTable_Proc已存在!!!");
                    }
                }
                //18.创建恢复TransactionOutput表的存储过程
                if (!procedureExist("Restore_TransactionOutputTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_TransactionOutputTable_Proc @BlockID bigint AS BEGIN DELETE FROM [dbo].[TransactionOutput] " +
                                        "WHERE TxID IN(SELECT TxID FROM [dbo].[Transaction] WHERE BlockID>@BlockID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionOutputTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_TransactionOutputTable_Proc已存在!!!");
                    }
                }
                //19.创建恢复OpreturnOutput表的存储过程
                if (!procedureExist("Restore_OpreturnOutputTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_OpreturnOutputTable_Proc @BlockID bigint AS BEGIN DELETE FROM [dbo].[OpreturnOutput] " +
                                        "WHERE TxID IN(SELECT TxID FROM [dbo].[Transaction] WHERE BlockID>@BlockID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_OpreturnOutputTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_OpreturnOutputTable_Proc已存在!!!");
                    }
                }                
                sqlConnection.Close();
            }
        }

        //c.分别判断表值是否存在，没有则创建
        public void tableTypeCreate(bool printMark)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand;
                //1.创建BlockTableType表值
                if (!tableTypeExist("BlockTableType"))
                {
                    string commandStr = "CREATE TYPE BlockTableType AS TABLE([BlockID][bigint] NOT NULL,[BlockHeight] [bigint] NOT NULL,[BlockHash] [binary](32) NOT NULL," +
                                        "[PreviousBlockHash] [binary](32) NOT NULL,[BlockTimestamp] [datetime] NOT NULL,[BlockVersion] [int] NOT NULL,[BlockSize] [bigint] NOT NULL," +
                                        "[Bits] [binary](4) NOT NULL,[Nonce] [binary](4) NOT NULL,[TxCount] [bigint] NOT NULL,[HashMerkleRoot] [binary](32) NOT NULL,[BlockchainFileId] [int] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("BlockTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("BlockTableType表值已存在!!!");
                    }
                }
                //2.创建TransactionTableType表值
                if (!tableTypeExist("TransactionTableType"))
                {
                    string commandStr = "CREATE TYPE TransactionTableType AS TABLE([TxID] [bigint] NOT NULL,[TxHash] [binary](32) NOT NULL,[BlockID] [bigint] NOT NULL," +
                                        "[TxVersion] [int] NOT NULL,[TxInCount] [bigint] NOT NULL,[TxOutCount][bigint] NOT NULL,[LockTime] [int] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("TransactionTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("TransactionTableType表值已存在!!!");
                    }
                }
                //3.创建TransactionInputTableType表值
                if (!tableTypeExist("TransactionInputTableType"))
                {
                    string commandStr = "CREATE TYPE TransactionInputTableType AS TABLE([TxInID][bigint] NOT NULL,[TxID] [bigint] NOT NULL," +
                                        "[SourceTxOutID] [bigint] NOT NULL,[Sequence] [bigint] NOT NULL,[InputScript] [varbinary](max)NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("TransactionInputTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("TransactionInputTableType表值已存在!!!");
                    }
                }
                //4.创建TransactionOutputTableType表值
                if (!tableTypeExist("TransactionOutputTableType"))
                {
                    string commandStr = "CREATE TYPE TransactionOutputTableType AS TABLE([TxOutID][bigint] NOT NULL,[TxID] [bigint] NOT NULL," +
                                        "[OutputIndex] [int] NOT NULL,[OutputValue] [bigint] NOT NULL,[OutputScript] [varbinary](max)NOT NULL,[AddressID] [bigint])";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("TransactionOutputTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("TransactionOutputTableType表值已存在!!!");
                    }
                }
                //5.创建OpreturnOutputTableType表值
                if (!tableTypeExist("OpreturnOutputTableType"))
                {
                    string commandStr = "CREATE TYPE OpreturnOutputTableType AS TABLE([OpreturnOutID][bigint] NOT NULL,[TxID] [bigint] NOT NULL," +
                                        "[OutputIndex] [int] NOT NULL,[OutputValue] [bigint] NOT NULL,[OutputScript] [varbinary](max)NOT NULL,[AddressID] [bigint])";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("OpreturnOutputTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("OpreturnOutputTableType表值已存在!!!");
                    }
                }
                //6.创建AddressTableType表值
                if (!tableTypeExist("AddressTableType"))
                {
                    string commandStr = "CREATE TYPE AddressTableType AS TABLE([AddressID][bigint] NOT NULL,[Address] [varchar](64) NOT NULL UNIQUE," +
                                        "[ClusterID] [bigint] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("AddressTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("AddressTableType表值已存在!!!");
                    }
                }
                sqlConnection.Close();
            }
        }
        
        //----4.数据库初始化(包括数据库连接检测、数据库表的创建和存储过程的创建)----
        public bool initialization_Database(bool printMark)
        {
            bool successMark = true;
            if (databaseConnectTest(printMark))
            {
                try
                {
                    blockTableCreate(printMark);
                    blockTableProcedureCreate(printMark);
                    tableTypeCreate(printMark);
                }
                catch (Exception e)
                {
                    successMark = false;
                    Console.WriteLine(e.Message);
                    throw e;
                }
            }
            else
            {
                successMark = false;
            }
            return successMark;
        }

        //----5.根据文件名获取文件编号----
        public int getBlockchainFileID(string blockchainFileName)
        {
            int blockchainFileID = -1;
            if (blockchainFileName != null && blockchainFileName != "")
            {
                blockchainFileID = Convert.ToInt32(blockchainFileName.Substring(3, 5));
            }
            return blockchainFileID;
        }

        ////II.*****数据库恢复*****
        //----1.获取对应表中最大的ID值(主键)----
        //a.获取ProcessedBlockID表中最大的ID
        public Int64 getMaxProcessedBlockID()
        {
            Int64 maxProcessedBlockID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxProcessedBlockID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxProcessedBlockIDSqlPar = new SqlParameter("@BlockID", SqlDbType.BigInt);
                maxProcessedBlockIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxProcessedBlockIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@BlockID"].Value == System.DBNull.Value)
                {
                    maxProcessedBlockID = -1;
                }
                else
                {
                    maxProcessedBlockID = Convert.ToInt64(cmd.Parameters["@BlockID"].Value);
                }
                sqlConnection.Close();
            }
            return maxProcessedBlockID;
        }

        //b.获取Block表中最大的ID
        public Int64 getMaxBlockID()
        {
            Int64 maxBlockID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxBlockID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxBlockIDSqlPar = new SqlParameter("@BlockID", SqlDbType.BigInt);
                maxBlockIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxBlockIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@BlockID"].Value == System.DBNull.Value)
                {
                    maxBlockID = -1;
                }
                else
                {
                    maxBlockID = Convert.ToInt64(cmd.Parameters["@BlockID"].Value);
                }
                sqlConnection.Close();
            }
            return maxBlockID;
        }

        //c.获取Transaction表中最大的ID
        public Int64 getMaxTransactionID()
        {
            Int64 maxTransactionID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxTransactionID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxTransactionIDSqlPar = new SqlParameter("@TxID", SqlDbType.BigInt);
                maxTransactionIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxTransactionIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@TxID"].Value == System.DBNull.Value)
                {
                    maxTransactionID = -1;
                }
                else
                {
                    maxTransactionID = Convert.ToInt64(cmd.Parameters["@TxID"].Value);
                }
                sqlConnection.Close();
            }
            return maxTransactionID;
        }

        //d.获取TransactionInput表中最大的ID
        public Int64 getMaxTransactionInputID()
        {
            Int64 maxTransactionInputID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxTransactionInputID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxTransactionInputIDSqlPar = new SqlParameter("@TxInID", SqlDbType.BigInt);
                maxTransactionInputIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxTransactionInputIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@TxInID"].Value == System.DBNull.Value)
                {
                    maxTransactionInputID = -1;
                }
                else
                {
                    maxTransactionInputID = Convert.ToInt64(cmd.Parameters["@TxInID"].Value);
                }
                sqlConnection.Close();
            }
            return maxTransactionInputID;
        }

        //e.获取TransactionOutput表中最大的ID
        public Int64 getMaxTransactionOutputID()
        {
            Int64 maxTransactionOutputID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxTransactionOutputID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxTransactionOutputIDSqlPar = new SqlParameter("@TxOutID", SqlDbType.BigInt);
                maxTransactionOutputIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxTransactionOutputIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@TxOutID"].Value == System.DBNull.Value)
                {
                    maxTransactionOutputID = -1;
                }
                else
                {
                    maxTransactionOutputID = Convert.ToInt64(cmd.Parameters["@TxOutID"].Value);
                }
                sqlConnection.Close();
            }
            return maxTransactionOutputID;
        }

        //f.获取TransactionOutput表中最大的ID
        public Int64 getMaxOpreturnOutputID()
        {
            Int64 maxOpreturnOutputID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxOpreturnOutputID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxOpreturnOutputIDSqlPar = new SqlParameter("@OpreturnOutID", SqlDbType.BigInt);
                maxOpreturnOutputIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxOpreturnOutputIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@OpreturnOutID"].Value == System.DBNull.Value)
                {
                    maxOpreturnOutputID = -1;
                }
                else
                {
                    maxOpreturnOutputID = Convert.ToInt64(cmd.Parameters["@OpreturnOutID"].Value);
                }
                sqlConnection.Close();
            }
            return maxOpreturnOutputID;
        }

        //g.获取Address表中最大的ID
        public Int64 getMaxAddressID()
        {
            Int64 maxAddressID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxAddressID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxAddressIDSqlPar = new SqlParameter("@AddressID", SqlDbType.BigInt);
                maxAddressIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxAddressIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@AddressID"].Value == System.DBNull.Value)
                {
                    maxAddressID = -1;
                }
                else
                {
                    maxAddressID = Convert.ToInt64(cmd.Parameters["@AddressID"].Value);
                }
                sqlConnection.Close();
            }
            return maxAddressID;
        }        

        //----2.恢复数据库中的对应表----
        //a.恢复Block表
        public void restore_Block_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_BlockTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //b.恢复Transaction表
        public void restore_Transaction_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_TransactionTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //c.恢复TransactionInput表
        public void restore_TransactionInput_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_TransactionInputTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //d.恢复TransactionOutput表
        public void restore_TransactionOutput_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_TransactionOutputTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //e.恢复OpreturnOutput表
        public void restore_OpreturnOutput_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_OpreturnOutputTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //f.恢复数据库的完整状态
        public void restore_DatabaseForBlockParserTable()
        {
            restore_TransactionInput_Table(getMaxProcessedBlockID());
            restore_TransactionOutput_Table(getMaxProcessedBlockID());
            restore_OpreturnOutput_Table(getMaxProcessedBlockID());
            restore_Transaction_Table(getMaxProcessedBlockID());
            restore_Block_Table(getMaxProcessedBlockID());
        }

        ////III.*****向数据库中写数据*****
        //----1.更新恢复表值表(Processed)的函数
        public void update_ProcessedBlockID_Table(Int64 processedBlockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 0;
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Update_ProcessedBlockID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@BlockID", processedBlockID);
                cmd.ExecuteNonQuery();

                sqlConnection.Close();
            }
        }

        //----2.获取Input的原交易输出SourceTxOutID的函数----
        public Int64 get_SourceTxOutID(string sourceTxhashAndIndex)
        {
            Int64 sourceTxOutID = bitcoinUTXOSlicer.utxoDictionary[sourceTxhashAndIndex].TxOutID[bitcoinUTXOSlicer.utxoDictionary[sourceTxhashAndIndex].utxoItemAmount - 1];
            return sourceTxOutID;
        }

        //----3.从数据库中获取下一个主键值的函数(数据库恢复后)----
        public void get_NextPrimaryKeyIDS(bool printMark)
        {
            nextBlockID = getMaxBlockID() + 1;
            nextTransactionID = getMaxTransactionID() + 1;
            nextTxInputID = getMaxTransactionInputID() + 1;
            nextTxOutputID = getMaxTransactionOutputID() + 1;
            nextOpreturnOutputID = getMaxOpreturnOutputID() + 1;
            if (printMark)
            {
                Console.WriteLine("nextBlockID:" + nextBlockID);
                Console.WriteLine("nextTransactionID:" + nextTransactionID);
                Console.WriteLine("nextTxInputID:" + nextTxInputID);
                Console.WriteLine("nextTxOutputID:" + nextTxOutputID);
                Console.WriteLine("nextOpreturnOutputID:" + nextOpreturnOutputID);
            }
        }

        //----4.表值类型创建函数----
        //a.创建Block的数据表值类型
        public DataTable get_Block_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] { 
                new DataColumn("BlockID",typeof(Int64)),//0    
                new DataColumn("BlockHeight",typeof(Int64)),//1
                new DataColumn("BlockHash",typeof(byte[])),//2
                new DataColumn("PreviousBlockHash",typeof(byte[])),//3
                new DataColumn("BlockTimestamp",typeof(DateTime)),//4
                new DataColumn("BlockVersion",typeof(int)),//5
                new DataColumn("BlockSize",typeof(int)),//6
                new DataColumn("Bits",typeof(byte[])),//7
                new DataColumn("Nonce",typeof(byte[])),//8
                new DataColumn("TxCount",typeof(int)),//9
                new DataColumn("HashMerkleRoot",typeof(byte[])),//10
                new DataColumn("BlockchainFileId",typeof(int))//11

            });
            return dt;
        }

        //b.创建Transaction的数据表值类型
        public DataTable get_Transaction_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("TxID",typeof(Int64)),//0    
                new DataColumn("TxHash",typeof(byte[])),//1
                new DataColumn("BlockID",typeof(Int64)),//2
                new DataColumn("TxVersion",typeof(int)),//3
                new DataColumn("TxInCount",typeof(Int64)),//4
                new DataColumn("TxOutCount",typeof(Int64)),//5
                new DataColumn("LockTime",typeof(int)),//6
            });
            return dt;
        }

        //c.创建TransactionInput的数据表值类型
        public DataTable get_TransactionInput_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("TxInID",typeof(Int64)),//0    
                new DataColumn("TxID",typeof(Int64)),//1
                new DataColumn("SourceTxOutID",typeof(Int64)),//2
                new DataColumn("Sequence",typeof(Int64)),//3
                new DataColumn("InputScript",typeof(byte[])),//4
            });
            return dt;
        }

        //d.创建TransactionOutput的数据表值类型
        public DataTable get_TransactionOutput_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("TxOutID",typeof(Int64)),//0    
                new DataColumn("TxID",typeof(Int64)),//1
                new DataColumn("OutputIndex",typeof(int)),//2
                new DataColumn("OutputValue",typeof(Int64)),//3
                new DataColumn("OutputScript",typeof(byte[])),//4
                new DataColumn("AddressID",typeof(Int64)),//5
            });
            return dt;
        }

        //e.创建TransactionOutput的数据表值类型
        public DataTable get_OpreturnOutput_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("OpreturnOutID",typeof(Int64)),//0    
                new DataColumn("TxID",typeof(Int64)),//1
                new DataColumn("OutputIndex",typeof(int)),//2
                new DataColumn("OutputValue",typeof(Int64)),//3
                new DataColumn("OutputScript",typeof(byte[])),//4
                new DataColumn("AddressID",typeof(Int64)),//5
            });
            return dt;
        }
        
        //f.获取Block的5个表值类型
        public void get_FiveTableSchema(out DataTable blockDataTable, out DataTable transactionDataTable,
        out DataTable transactionInputDataTable, out DataTable transactionOutputDataTable, out DataTable opreturnOutputDataTable)
        {
            blockDataTable = get_Block_TableSchema();
            transactionDataTable = get_Transaction_TableSchema();
            transactionInputDataTable = get_TransactionInput_TableSchema();
            transactionOutputDataTable = get_TransactionOutput_TableSchema();
            opreturnOutputDataTable = get_OpreturnOutput_TableSchema();
        }

        //----5.表值数据添加函数----
        //a.添加一条记录到Block的数据表值类型
        public void addOneRowToBlockDataTable(DataTable dataTable, ParserBlock parserBlock, Int64 blockID, Int64 blockHeight, bool LittleEndian)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = blockID;
            dataRow[1] = blockHeight;
            dataRow[4] = parserBlock.Header.BlockTime.DateTime;
            dataRow[5] = parserBlock.Header.Version;
            dataRow[6] = parserBlock.BlockLength;
            dataRow[9] = parserBlock.Transactions.Count;
            dataRow[11] = getBlockchainFileID(parserBlock.BlockchainFileName);
            if (LittleEndian)
            {
                //小字节序存储
                dataRow[2] = parserBlock.Header.GetHash().ToBytes();
                dataRow[3] = parserBlock.Header.HashPrevBlock.ToBytes();
                dataRow[7] = BitConverter.GetBytes(parserBlock.Header.Bits);
                dataRow[8] = BitConverter.GetBytes(parserBlock.Header.Nonce);
                dataRow[10] = parserBlock.GetMerkleRoot().Hash.ToBytes();
            }
            else
            {
                //大字节序存储
                byte[] blockHash = parserBlock.Header.GetHash().ToBytes();
                byte[] previousBlockHashByteArr = parserBlock.Header.HashPrevBlock.ToBytes();
                byte[] bitsByteArr = BitConverter.GetBytes(parserBlock.Header.Bits);
                byte[] nonceByteArr = BitConverter.GetBytes(parserBlock.Header.Nonce);
                byte[] hashMerkleRootByteArr = parserBlock.GetMerkleRoot().Hash.ToBytes();
                Array.Reverse(blockHash);
                Array.Reverse(previousBlockHashByteArr);
                Array.Reverse(bitsByteArr);
                Array.Reverse(nonceByteArr);
                Array.Reverse(hashMerkleRootByteArr);
                dataRow[2] = blockHash;
                dataRow[3] = previousBlockHashByteArr;
                dataRow[7] = bitsByteArr;
                dataRow[8] = nonceByteArr;
                dataRow[10] = hashMerkleRootByteArr;
            }
            dataTable.Rows.Add(dataRow);
        }

        //b.添加一条记录到Transaction的数据表值类型
        public void addOneRowToTransactionDataTable(DataTable dataTable, Transaction transaction, Int64 txID, Int64 blockID, bool LittleEndian)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = txID;
            dataRow[2] = blockID;
            dataRow[3] = (int)transaction.Version;
            dataRow[4] = transaction.IsCoinBase ? 0 : transaction.Inputs.Count;
            dataRow[5] = transaction.Outputs.Count;
            dataRow[6] = (int)transaction.LockTime.Value;
            if (LittleEndian)
            {
                dataRow[1] = transaction.GetHash().ToBytes();
            }
            else
            {
                byte[] txHash = transaction.GetHash().ToBytes();
                Array.Reverse(txHash);
                dataRow[1] = txHash;
            }
            dataTable.Rows.Add(dataRow);
        }

        //c.添加一条记录到TransactionInput的数据表值类型
        public void addOneRowToTransactionInputDataTable(DataTable dataTable, TxIn txIn, Int64 txInID, Int64 txID, Int64 sourceTxOutID, bool LittleEndian)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = txInID;
            dataRow[1] = txID;
            dataRow[2] = sourceTxOutID;
            dataRow[3] = Convert.ToInt64(txIn.Sequence.Value);
            if(LittleEndian)
            {
                dataRow[4] = txIn.ScriptSig.ToBytes();
            }
            else
            {
                byte[] inputScriptByteArr = txIn.ScriptSig.ToBytes();
                Array.Reverse(inputScriptByteArr);
                dataRow[4] = inputScriptByteArr;
            }
            dataTable.Rows.Add(dataRow);
        }

        //d.添加一条记录到TransactionOutput的数据表值类型
        public void addOneRowToTransactionOutputDataTable(DataTable dataTable, TxOut txOut, Int64 txOutID, Int64 txID, int outputIndex, bool LittleEndian)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = txOutID;
            dataRow[1] = txID;
            dataRow[2] = outputIndex;
            dataRow[3] = txOut.Value.Satoshi;
            dataRow[5] = 0;
            if (LittleEndian)
            {
                dataRow[4] = txOut.ScriptPubKey.ToBytes();
            }
            else
            {
                byte[] outputScriptByteArr = txOut.ScriptPubKey.ToBytes();
                Array.Reverse(outputScriptByteArr);
                dataRow[4] = outputScriptByteArr;
            }
            dataTable.Rows.Add(dataRow);
        }

        //e.添加一条记录到OpreturnOutput的数据表值类型
        public void addOneRowToOpreturnOutputDataTable(DataTable dataTable, TxOut txOut, Int64 opreturnOutID, Int64 txID, int outputIndex, bool LittleEndian)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = opreturnOutID;
            dataRow[1] = txID;
            dataRow[2] = outputIndex;
            dataRow[3] = txOut.Value.Satoshi;
            dataRow[5] = 0;
            if (LittleEndian)
            {
                dataRow[4] = txOut.ScriptPubKey.ToBytes();
            }
            else
            {
                byte[] outputScriptByteArr = txOut.ScriptPubKey.ToBytes();
                Array.Reverse(outputScriptByteArr);
                dataRow[4] = outputScriptByteArr;
            }
            dataTable.Rows.Add(dataRow);
        }

        //f.将一个交易的输入记录全部添加到TransactionInput的数据表值类型
        public void addAllTxInputToTransactionInputDataTable(DataTable dataTable, Transaction transaction, Int64 txID, bool LittleEndian)
        {
            if (!transaction.IsCoinBase)
            {
                foreach (TxIn txIn in transaction.Inputs)
                {
                    string sourceTxhashAndIndex = txIn.PrevOut.ToString();
                    //Console.WriteLine(transaction.GetHash()+"***"+ sourceTxhashAndIndex);
                    Int64 sourceTxOutID = get_SourceTxOutID(sourceTxhashAndIndex);
                    addOneRowToTransactionInputDataTable(dataTable,txIn, nextTxInputID, txID, sourceTxOutID, LittleEndian);
                    nextTxInputID++;
                }
            }
        }

        //g.将一个交易的输出记录全部添加到TransactionOutput数据表和OpreturnOutput数据表值类型
        public void addAllTxOutputToOutputDataTable(DataTable outputDataTable, DataTable opreturnDataTable, Transaction transaction, Int64 txID, bool LittleEndian)
        {
            int outputIndex = 0;
            foreach (TxOut txOut in transaction.Outputs)
            {
                ulong value = txOut.Value;
                if (isOpreturn(txOut))
                {
                    addOneRowToOpreturnOutputDataTable(opreturnDataTable, txOut, nextOpreturnOutputID, txID, outputIndex, LittleEndian);
                    nextOpreturnOutputID++;
                }
                else
                {
                    addOneRowToTransactionOutputDataTable(outputDataTable, txOut, nextTxOutputID, txID, outputIndex, LittleEndian);
                    nextTxOutputID++;
                }             
                outputIndex++;
            }
        }

        //h.将一个交易的交易记录、所有输入记录、所有输出记录全部添加到对应的数据表值类型中
        public void addAllTxDataToDataTable(DataTable transactionDataTable,DataTable inputDataTable,DataTable outputDataTable,DataTable opreturnDataTable, Transaction transaction, Int64 blockID, bool LittleEndian)
        {
            addOneRowToTransactionDataTable(transactionDataTable,transaction, nextTransactionID, blockID, LittleEndian);
            addAllTxInputToTransactionInputDataTable(inputDataTable, transaction, nextTransactionID, LittleEndian);
            if (bitcoinUTXOSlicer.isValidTransaction(transaction))
            {
                addAllTxOutputToOutputDataTable(outputDataTable, opreturnDataTable,transaction, nextTransactionID, LittleEndian);
            }
            nextTransactionID++;
        }

        //i.判断opreturn
        public bool isOpreturn(TxOut txOut)
        {
            bool opreturnMark = false;
            int scriptLen = txOut.ScriptPubKey.ToBytes().Length;
            if (scriptLen >= 1)
            {
                if (txOut.ScriptPubKey.ToBytes()[0] == 0x6a)
                {
                    opreturnMark = true;
                }
                else
                {
                    if (scriptLen >= 2)
                    {
                        if (txOut.ScriptPubKey.ToBytes()[0] == 0x00 && txOut.ScriptPubKey.ToBytes()[1] == 0x6a)
                        {
                            opreturnMark = true;
                        }
                    }
                }
            }
            return opreturnMark;
        }

        //----6.表值入库函数----
        //a.将Block表值写入到数据库中
        public void blockTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection=new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[Block] (BlockID,BlockHeight,BlockHash,PreviousBlockHash,BlockTimestamp,BlockVersion,BlockSize,Bits,Nonce,TxCount,HashMerkleRoot,BlockchainFileId)" +
                                        "SELECT nc.BlockID,nc.BlockHeight,nc.BlockHash,nc.PreviousBlockHash,nc.BlockTimestamp,nc.BlockVersion,nc.BlockSize,nc.Bits,nc.Nonce,nc.TxCount,nc.HashMerkleRoot,nc.BlockchainFileId FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                cmd.CommandTimeout = 0;
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[BlockTableType]";
                try
                {
                    sqlConnection.Open();
                    if (dataTable != null && dataTable.Rows.Count != 0)
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sqlConnection.Close();
                }  
            }
        }

        //b.将Transaction表值写入到数据库中
        public void transactionTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[Transaction] (TxID, TxHash, BlockID, TxVersion, TxInCount, TxOutCount, LockTime)" +
                                        "SELECT nc.TxID, nc.TxHash, nc.BlockID, nc.TxVersion, nc.TxInCount, nc.TxOutCount, nc.LockTime FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                cmd.CommandTimeout = 0;
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[TransactionTableType]";
                try
                {
                    sqlConnection.Open();
                    if (dataTable != null && dataTable.Rows.Count != 0)
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
        }

        //c.将TransactionInput表值写入到数据库中
        public void transactionInputTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[TransactionInput] (TxInID, TxID, SourceTxOutID, Sequence, InputScript)" +
                                        "SELECT nc.TxInID, nc.TxID, nc.SourceTxOutID, nc.Sequence, nc.InputScript FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                cmd.CommandTimeout = 0;
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[TransactionInputTableType]";
                try
                {
                    sqlConnection.Open();
                    if (dataTable != null && dataTable.Rows.Count != 0)
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
        }

        //d.将TransactionOutput表值写入到数据库中
        public void transactionOutputTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[TransactionOutput] (TxOutID, TxID, OutputIndex, OutputValue, OutputScript, AddressID)" +
                                        "SELECT nc.TxOutID, nc.TxID, nc.OutputIndex, nc.OutputValue, nc.OutputScript,nc.AddressID FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                cmd.CommandTimeout = 0;
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[TransactionOutputTableType]";
                try
                {
                    sqlConnection.Open();
                    if (dataTable != null && dataTable.Rows.Count != 0)
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
        }

        //e.将TransactionOutput表值写入到数据库中
        public void opreturnOutputTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[OpreturnOutput] (OpreturnOutID, TxID, OutputIndex, OutputValue, OutputScript, AddressID)" +
                                        "SELECT nc.OpreturnOutID, nc.TxID, nc.OutputIndex, nc.OutputValue, nc.OutputScript,nc.AddressID FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                cmd.CommandTimeout = 0;
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[OpreturnOutputTableType]";
                try
                {
                    sqlConnection.Open();
                    if (dataTable != null && dataTable.Rows.Count != 0)
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
        }

        //f.将Block的5个表值中的数据写到数据库中
        public void fiveTableValuedToDB(DataTable blockDataTable,DataTable transactionDataTable,
        DataTable transactionInputDataTable,DataTable transactionOutputDataTable,DataTable opreturnOutputDataTable)
        {
            blockTableValuedToDB(blockDataTable);
            transactionTableValuedToDB(transactionDataTable);
            transactionInputTableValuedToDB(transactionInputDataTable);
            transactionOutputTableValuedToDB(transactionOutputDataTable);
            opreturnOutputTableValuedToDB(opreturnOutputDataTable);
        }

        ////IV.****本项目的最终调用运行函数****
        //运行程序将解析数据写到数据库中
        public void run()
        {
            Console.WriteLine("开始执行.........");
            bool littleEndian = true;
            get_NextPrimaryKeyIDS(true);
            bitcoinUTXOSlicer.get_NextPrimaryKeyIDS(true);
            bitcoinUTXOSlicer.nextTxOutputID = nextTxOutputID;//保证UTXOSlicer和ParserToDatabase中的nextTxOutputID相一致
            DataTable blockDataTable = get_Block_TableSchema();
            DataTable transactionDataTable = get_Transaction_TableSchema();
            DataTable transactionInputDataTable = get_TransactionInput_TableSchema();
            DataTable transactionOutputDataTable = get_TransactionOutput_TableSchema();
            DataTable opreturnOutputDataTable = get_OpreturnOutput_TableSchema();
            ParserBlock readyBlock;
            DateTime dateTime = DateTime.Now;
            while ((readyBlock = bitcoinUTXOSlicer.get_NextParserBlock()) != null)
            {
                if (nextBlockID == bitcoinUTXOSlicer.orderedBlockchainParser.processedBlockAmount - 1)
                {                                       
                    addOneRowToBlockDataTable(blockDataTable,readyBlock, nextBlockID, nextBlockID, littleEndian);
                    foreach (Transaction transaction in readyBlock.Transactions)
                    {
                        addAllTxDataToDataTable(transactionDataTable, transactionInputDataTable, transactionOutputDataTable, opreturnOutputDataTable, transaction, nextBlockID, littleEndian);
                        bitcoinUTXOSlicer.updateUTXO_ForOneTransaction(transaction);
                    }
                    if (bitcoinUTXOSlicer.orderedBlockchainParser.processedBlockAmount % 100 == 0) 
                    {
                        DateTime currentDateTime = DateTime.Now;
                        TimeSpan timeSpan = currentDateTime - dateTime;
                        Console.WriteLine("处理100个块用时:"+ timeSpan);
                        dateTime = currentDateTime;
                    }                    
                    if (bitcoinUTXOSlicer.orderedBlockchainParser.processedBlockAmount % 100 == 0)
                    {                        
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        fiveTableValuedToDB(blockDataTable, transactionDataTable, transactionInputDataTable, transactionOutputDataTable, opreturnOutputDataTable);
                        update_ProcessedBlockID_Table(bitcoinUTXOSlicer.orderedBlockchainParser.processedBlockAmount - 1);
                        sw.Stop();
                        Console.WriteLine("区块解析数据本次写库用时:"+sw.Elapsed);
                        Console.WriteLine("已处理" + bitcoinUTXOSlicer.orderedBlockchainParser.processedBlockAmount + "个区块");
                        Console.WriteLine("当前区块时间:" + bitcoinUTXOSlicer.nextParserBlock.Header.BlockTime.DateTime);
                        Console.WriteLine("相同交易出现次数:" + bitcoinUTXOSlicer.sameTransactionCount);
                        Console.WriteLine("***********************");
                        get_FiveTableSchema(out blockDataTable, out transactionDataTable, out transactionInputDataTable, out transactionOutputDataTable, out opreturnOutputDataTable);
                    }
                    bitcoinUTXOSlicer.save_AllProgramContextFile(nextBlockID);
                    nextBlockID++;
                    if (bitcoinUTXOSlicer.terminationConditionJudment())
                    {
                        break;
                    }
                }
                else
                {
                    foreach (Transaction transaction in readyBlock.Transactions)
                    {
                        bitcoinUTXOSlicer.updateUTXO_ForOneTransaction(transaction);
                    }
                }
            }
            Console.WriteLine("执行结束.........");
        }        
    }
}
