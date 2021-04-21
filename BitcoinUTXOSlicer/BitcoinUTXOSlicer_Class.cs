using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Text;
using BitcoinBlockchain.Data;
using NBitcoin;
using Newtonsoft.Json;
using OrderedBitcoinBlockchainParser;

namespace BitcoinUTXOSlicer
{
    public class BitcoinUTXOSlicer_Class
    {
        //和OrderedBlockchainParser_Class相关成员
        public OrderedBitcoinBlockchainParser_Class orderedBlockchainParser;
        public string blockchainFilePath = @".\";               //区块链文件路径        
        public string blockProcessContextFilePath = @".\";      //区块解析中断时的上下文(程序状态)文件路径
        public string blockProcessContextFileName = null;       //区块解析中断时的上下文(程序状态)文件
        public int blockQueuePoolingSize = Configuration_Class.blockQueuePoolingSize;//区块池长度(正在修改)
        //新加入成员
        string UtxoSliceFilePath = @".\";                       //utxo切片文件存储路径
        string OpReturnFilePath = null;                         //opreturn文件存储路径
        string UtxoSliceFileName = null;                        //utxo切片恢复文件名
        string sliceIntervalTimeType;                           //切片间隔类型(year month day)
        int sliceIntervalTime;                                  //切片间隔长度       
        DateTime endTime = DateTime.MaxValue;                   //时间中止条件
        int endBlockHeight = int.MaxValue;                      //区块高度终止条件
        //原有成员
        public Dictionary<string, UTXOItem_Class> utxoDictionary = new Dictionary<string, UTXOItem_Class>();
        public LinkedList<opreturnOutputItem_Class> opreturnOutputLinkedList = new LinkedList<opreturnOutputItem_Class>();
        public int sliceFileAmount = 0;
        public int sameTransactionCount = 0;
        //最近的切片时间
        DateTime recentlySliceDateTime;
        //写库相关的成员
        public int nextSliceID;
        public Int64 nextSliceRecordID;
        public Int64 nextTxOutputID;
        public Int64 nextOpreturnOutputID;
        public ParserBlock nextParserBlock;
        string sqlConnectionString = null;

        public BitcoinUTXOSlicer_Class() { }
        //不带写库功能的构造
        public BitcoinUTXOSlicer_Class(string blockchainFilePath, string blockProcessContextFilePath, string blockProcessContextFileName,
                                string UtxoSliceFilePath, string UtxoSliceFileName, string OpReturnFilePath,
                                string sliceIntervalTimeType, int sliceIntervalTime, DateTime endTime, int endBlockHeight)
        {
            this.blockchainFilePath = blockchainFilePath;                   //区块链文件路径
            this.blockProcessContextFilePath = blockProcessContextFilePath; //区块解析上下文文件存储路径
            this.blockProcessContextFileName = blockProcessContextFileName; //区块解析上下文恢复文件名
            this.UtxoSliceFilePath = UtxoSliceFilePath;                     //utxo切片文件存储路径
            this.UtxoSliceFileName = UtxoSliceFileName;                     //utxo切片恢复文件名
            this.OpReturnFilePath = OpReturnFilePath;                       //opreturn文件存储路径
            this.sliceIntervalTimeType = sliceIntervalTimeType;             //切片间隔类型
            this.sliceIntervalTime = sliceIntervalTime;                     //切片间隔长度            
            this.endTime = endTime;                                         //时间中止条件
            this.endBlockHeight = endBlockHeight;                           //区块高度终止条件
            parameter_Detection();                                          //参数检查

            if (UtxoSliceFileName == null || blockProcessContextFileName == null)
            { //从第0个块开始
                orderedBlockchainParser = new OrderedBitcoinBlockchainParser_Class(blockchainFilePath, blockProcessContextFilePath, null);
                recentlySliceDateTime = orderedBlockchainParser.recentlySliceDateTime;
            }
            else
            { //从中断处恢复
                orderedBlockchainParser = new OrderedBitcoinBlockchainParser_Class(blockchainFilePath, blockProcessContextFilePath, blockProcessContextFileName);
                recentlySliceDateTime = orderedBlockchainParser.recentlySliceDateTime;
                restore_UTXOSlicerContextForProgram();
            }
        }
        //可以写库的构造
        public BitcoinUTXOSlicer_Class(string blockchainFilePath, string blockProcessContextFilePath, string blockProcessContextFileName,
                                string UtxoSliceFilePath, string UtxoSliceFileName, string OpReturnFilePath,
                                string sliceIntervalTimeType, int sliceIntervalTime, DateTime endTime, int endBlockHeight, string sqlConnectionString)
        {
            this.blockchainFilePath = blockchainFilePath;                   //区块链文件路径
            this.blockProcessContextFilePath = blockProcessContextFilePath; //区块解析上下文文件存储路径
            this.blockProcessContextFileName = blockProcessContextFileName; //区块解析上下文恢复文件名
            this.UtxoSliceFilePath = UtxoSliceFilePath;                     //utxo切片文件存储路径
            this.UtxoSliceFileName = UtxoSliceFileName;                     //utxo切片恢复文件名
            this.OpReturnFilePath = OpReturnFilePath;                       //opreturn文件存储路径
            this.sliceIntervalTimeType = sliceIntervalTimeType;             //切片间隔类型
            this.sliceIntervalTime = sliceIntervalTime;                     //切片间隔长度            
            this.endTime = endTime;                                         //时间中止条件
            this.endBlockHeight = endBlockHeight;                           //区块高度终止条件
            this.sqlConnectionString = sqlConnectionString;                 //数据库连接字符串
            parameter_Detection();                                          //参数检查

            initialization_Database(true);
            if (UtxoSliceFileName == null || blockProcessContextFileName == null)
            { //从第0个块开始
                orderedBlockchainParser = new OrderedBitcoinBlockchainParser_Class(blockchainFilePath, blockProcessContextFilePath, null);
                recentlySliceDateTime = orderedBlockchainParser.recentlySliceDateTime;
            }
            else
            { //从中断处恢复
                orderedBlockchainParser = new OrderedBitcoinBlockchainParser_Class(blockchainFilePath, blockProcessContextFilePath, blockProcessContextFileName);
                recentlySliceDateTime = orderedBlockchainParser.recentlySliceDateTime;
                restore_UTXOSlicerContextForProgram();
                Console.WriteLine("正在恢复UTXOSliceTable......");                
            }
            restore_SliceUTXO_Table();
        }

        ////I.****切片程序运行相关函数****
        //----1.获取下一个区块(新增)----
        public ParserBlock get_NextParserBlock()
        {
            nextParserBlock = orderedBlockchainParser.getNextBlock();
            return nextParserBlock;
        }

        //----2.交易执行相关的函数----
        //a.验证交易是否合法
        public bool isValidTransaction(Transaction transaction)
        {
            if (!transaction.IsCoinBase)
            {
                ulong totalInputValue = 0;
                foreach (TxIn transactionInput in transaction.Inputs)
                {
                    string sourceTxhashAndIndex = transactionInput.PrevOut.ToString();
                    if (utxoDictionary.ContainsKey(sourceTxhashAndIndex))
                    {
                        totalInputValue += utxoDictionary[sourceTxhashAndIndex].value;
                    }
                    else
                    {
                        return false;
                    }
                }
                ulong totalOutputValue = 0;
                foreach (TxOut transactionOutput in transaction.Outputs)
                {
                    totalOutputValue += transactionOutput.Value;
                }
                if (totalInputValue < totalOutputValue)
                {
                    return false;
                }
            }
            return true;
        }

        //b.执行铸币交易(更新)
        public void execute_CoinbaseTransaction(Transaction transaction)
        {
            uint indexOfOutput = 0;
            foreach (TxOut transactionOutput in transaction.Outputs)
            {
                string txhashAndIndex = transaction.GetHash().ToString() + "-" + indexOfOutput;
                string txhash = transaction.GetHash().ToString();
                ulong value = transactionOutput.Value;
                string script = new ByteArray(transactionOutput.ScriptPubKey.ToBytes()).ToString();
                if (value == 0)
                {
                    if (transactionOutput.ScriptPubKey.ToBytes()[0] == 0x6a || transactionOutput.ScriptPubKey.ToBytes()[1] == 0x6a)
                    {
                        opreturnOutputItem_Class opreturnOutputItem = new opreturnOutputItem_Class(txhash, indexOfOutput, value, script);
                        opreturnOutputLinkedList.AddLast(opreturnOutputItem);
                    }
                    else
                    {
                        UTXOItem_Class unSpentTxOutItem = new UTXOItem_Class(txhash, indexOfOutput, value, script);//可以放在if里面                        
                        if (!utxoDictionary.ContainsKey(txhashAndIndex))
                        {
                            unSpentTxOutItem.TxOutID.Add(nextTxOutputID);
                            utxoDictionary.Add(txhashAndIndex, unSpentTxOutItem);
                        }
                        else
                        {
                            utxoDictionary[txhashAndIndex].TxOutID.Add(nextTxOutputID);
                            utxoDictionary[txhashAndIndex].utxoItemAmount++;
                            sameTransactionCount++;
                        }
                        nextTxOutputID++;
                    }
                }
                else
                {
                    UTXOItem_Class unSpentTxOutItem = new UTXOItem_Class(txhash, indexOfOutput, value, script);//可以放在if里面
                    if (!utxoDictionary.ContainsKey(txhashAndIndex))
                    {
                        unSpentTxOutItem.TxOutID.Add(nextTxOutputID);
                        utxoDictionary.Add(txhashAndIndex, unSpentTxOutItem);
                    }
                    else
                    {
                        utxoDictionary[txhashAndIndex].TxOutID.Add(nextTxOutputID);
                        utxoDictionary[txhashAndIndex].utxoItemAmount++;
                        sameTransactionCount++;
                    }
                    nextTxOutputID++;
                }
                indexOfOutput++;
            }
        }

        //c.执行常规交易(更新)
        public void execute_RegularTransaction(Transaction transaction)
        {
            foreach (TxIn transactionInput in transaction.Inputs)
            {
                string sourceTxhashAndIndex = transactionInput.PrevOut.ToString();
                if (utxoDictionary.ContainsKey(sourceTxhashAndIndex))
                {
                    if (utxoDictionary[sourceTxhashAndIndex].utxoItemAmount > 1)
                    {
                        utxoDictionary[sourceTxhashAndIndex].utxoItemAmount--;
                        utxoDictionary[sourceTxhashAndIndex].TxOutID.RemoveAt(utxoDictionary[sourceTxhashAndIndex].TxOutID.Count - 1);
                    }
                    else
                    {
                        utxoDictionary.Remove(sourceTxhashAndIndex);
                    }

                }
                else
                {
                    Console.WriteLine("当前交易中的输入不存在:" + sourceTxhashAndIndex);
                    return;
                }
            }
            uint indexOfOutput = 0;
            foreach (TxOut transactionOutput in transaction.Outputs)
            {
                string txhashAndIndex = transaction.GetHash().ToString() + "-" + indexOfOutput;
                string txhash = transaction.GetHash().ToString();
                ulong value = transactionOutput.Value;
                string script = new ByteArray(transactionOutput.ScriptPubKey.ToBytes()).ToString();
                if (value == 0)
                {
                    if (transactionOutput.ScriptPubKey.ToBytes()[0] == 0x6a || transactionOutput.ScriptPubKey.ToBytes()[1] == 0x6a)
                    {
                        opreturnOutputItem_Class opreturnOutputItem = new opreturnOutputItem_Class(txhash, indexOfOutput, value, script);
                        opreturnOutputLinkedList.AddLast(opreturnOutputItem);
                    }
                    else
                    {
                        UTXOItem_Class unSpentTxOutItem = new UTXOItem_Class(txhash, indexOfOutput, value, script);//可以放在if里面
                        if (!utxoDictionary.ContainsKey(txhashAndIndex))
                        {
                            unSpentTxOutItem.TxOutID.Add(nextTxOutputID);
                            utxoDictionary.Add(txhashAndIndex, unSpentTxOutItem);
                        }
                        else
                        {
                            utxoDictionary[txhashAndIndex].TxOutID.Add(nextTxOutputID);
                            utxoDictionary[txhashAndIndex].utxoItemAmount++;
                            sameTransactionCount++;
                        }
                        nextTxOutputID++;
                    }
                }
                else
                {
                    UTXOItem_Class unSpentTxOutItem = new UTXOItem_Class(txhash, indexOfOutput, value, script);//可以放在if里面
                    if (!utxoDictionary.ContainsKey(txhashAndIndex))
                    {
                        unSpentTxOutItem.TxOutID.Add(nextTxOutputID);
                        utxoDictionary.Add(txhashAndIndex, unSpentTxOutItem);
                    }
                    else
                    {
                        utxoDictionary[txhashAndIndex].TxOutID.Add(nextTxOutputID);
                        utxoDictionary[txhashAndIndex].utxoItemAmount++;
                        sameTransactionCount++;
                    }
                    nextTxOutputID++;
                }
                indexOfOutput++;
            }
        }

        //----3.更新UTXO状态/每执行一个交易(新增)----
        public void updateUTXO_ForOneTransaction(Transaction transaction)
        {
            if (transaction.IsCoinBase)
            {
                execute_CoinbaseTransaction(transaction);
            }
            else
            {
                if (isValidTransaction(transaction))
                {
                    execute_RegularTransaction(transaction);
                }
            }
        }

        //----4.切片文件保存相关的函数----
        //a.保存切片状态
        public void save_SliceFile(int processedBlockAmount, int sliceFileAmount, DateTime endBlockTime)
        {
            string UtxoSliceFileFinalPath = Path.Combine(UtxoSliceFilePath, "UtxoSlice_" + processedBlockAmount + "_" + endBlockTime.ToString("yyyy年MM月dd日HH时mm分ss秒") + ".dat");
            SliceFileItem_Class sliceFileItem = new SliceFileItem_Class(utxoDictionary, sliceFileAmount, sameTransactionCount);
            using (StreamWriter sw = File.CreateText(UtxoSliceFileFinalPath))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(sw, sliceFileItem);
            }
            orderedBlockchainParser.Compress(UtxoSliceFileFinalPath, true);
        }

        //b.保存opreturn切片状态
        public void save_opreturnOutputsFile(int processedBlockAmount, DateTime endBlockTime)
        {
            string OpReturnFileFinalPath = Path.Combine(OpReturnFilePath, "OpReturn_" + processedBlockAmount + "_" + endBlockTime.ToString("yyyy年MM月dd日HH时mm分ss秒") + ".dat");
            using (StreamWriter sw = File.CreateText(OpReturnFileFinalPath))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(sw, opreturnOutputLinkedList);
            }
            orderedBlockchainParser.Compress(OpReturnFileFinalPath, true);
            opreturnOutputLinkedList = new LinkedList<opreturnOutputItem_Class>();
        }

        //c.切片条件判断函数
        public bool endConditionJudgment(DateTime newlyRecentlyDatetime, DateTime recentlyBlockDatetime)
        {
            TimeSpan timeSpan = recentlyBlockDatetime - newlyRecentlyDatetime;
            if (sliceIntervalTimeType == "year")
            {
                double amountOfYear = timeSpan.TotalDays / 365;
                if (amountOfYear >= (double)sliceIntervalTime)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (sliceIntervalTimeType == "month")
            {
                double amountOfMonth = timeSpan.TotalDays / 30;
                if (amountOfMonth >= (double)sliceIntervalTime)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (sliceIntervalTimeType == "day")
            {
                if (timeSpan.TotalDays >= (double)sliceIntervalTime)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                Console.WriteLine("请输入正确的切片间隔类型!!!(year/month/day)");
                return false;
            }
        }

        //----5.保存BPC和UTXOSlice文件(新增)----
        public void save_AllProgramContextFile(Int64 nextBlockID)
        {
            if (endConditionJudgment(recentlySliceDateTime, nextParserBlock.Header.BlockTime.DateTime))
            {
                sliceFileAmount++;
                Stopwatch sw = new Stopwatch();
                DataTable sliceUTXODataTable = get_SliceUTXO_TableSchema();
                sw.Start();
                allSliceDataToDB(sliceUTXODataTable, nextBlockID);
                sw.Stop();
                Console.WriteLine("切片数据写库用时:" + sw.Elapsed);
                Console.WriteLine("正在保存第" + sliceFileAmount + "个切片状态,请勿现在终止程序..........");
                Stopwatch sw1 = new Stopwatch();
                sw1.Start();
                save_SliceFile(orderedBlockchainParser.processedBlockAmount, sliceFileAmount, nextParserBlock.Header.BlockTime.DateTime);
                sw1.Stop();
                Console.WriteLine("UTXO切片保存完成");
                Console.WriteLine("保存第" + sliceFileAmount + "个切片用时:" + sw1.Elapsed);
                Console.WriteLine("正在保存第" + sliceFileAmount + "个程序上下文状态,请勿现在终止程序..........");
                orderedBlockchainParser.saveBlockProcessContext();
                Console.WriteLine("程序上下文保存完成");
                if (OpReturnFilePath != null)
                {
                    Console.WriteLine("正在保存第" + sliceFileAmount + "个opreturn切片状态,请勿现在终止程序..........");
                    save_opreturnOutputsFile(orderedBlockchainParser.processedBlockAmount, nextParserBlock.Header.BlockTime.DateTime);
                    Console.WriteLine("opreturn切片保存完成");
                }
                recentlySliceDateTime = nextParserBlock.Header.BlockTime.DateTime;
            }
        }

        //----6.终止条件判断(新增)----
        public bool terminationConditionJudment()
        {
            bool terminationMark = false;
            if (nextParserBlock.Header.BlockTime.DateTime >= endTime)
            {
                Console.WriteLine("当前区块时间:" + nextParserBlock.Header.BlockTime.DateTime);
                Console.WriteLine("当前区块高度:" + (orderedBlockchainParser.processedBlockAmount - 1));
                Console.WriteLine("触发时间终止条件，执行结束!!!");
                terminationMark = true;
            }
            if (orderedBlockchainParser.processedBlockAmount - 1 >= endBlockHeight)
            {
                Console.WriteLine("当前区块时间:" + nextParserBlock.Header.BlockTime.DateTime);
                Console.WriteLine("当前区块高度:" + (orderedBlockchainParser.processedBlockAmount - 1));
                Console.WriteLine("触发区块高度终止条件，执行结束!!!");
                terminationMark = true;
            }
            return terminationMark;
        }

        //----7.恢复UTXOSlicer上下文----
        public void restore_UTXOSlicerContextForProgram()
        {
            string utxoSlicerContextFileFinalPath = Path.Combine(UtxoSliceFilePath, UtxoSliceFileName);
            //判断给定文件名是压缩文件还是txt文件
            FileInfo fileName = new FileInfo(utxoSlicerContextFileFinalPath);
            if (fileName.Extension == ".rar")
            {
                Console.WriteLine("正在解压UtxoSlice下文状态文件......");
                orderedBlockchainParser.Decompress(utxoSlicerContextFileFinalPath, false);
                utxoSlicerContextFileFinalPath = Path.Combine(UtxoSliceFilePath, Path.GetFileNameWithoutExtension(utxoSlicerContextFileFinalPath));
            }
            if (File.Exists(utxoSlicerContextFileFinalPath))
            {
                //1.反序列化UtxoSlice文件
                Console.WriteLine("开始提取程序上下文状态文件数据(UtxoSlice).........");
                SliceFileItem_Class sliceFileItemObject = null;
                Stopwatch timer = new Stopwatch();
                timer.Start();
                try
                {
                    using (StreamReader sr = File.OpenText(utxoSlicerContextFileFinalPath))
                    {
                        JsonSerializer jsonSerializer = new JsonSerializer();
                        sliceFileItemObject = jsonSerializer.Deserialize(sr, typeof(SliceFileItem_Class)) as SliceFileItem_Class;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("UtxoSlice文件保存不完整或已经损坏。该错误可能是由于在保存UtxoSlice文件时提前终止程序造成的，或是人为修改了最近的UtxoSlice文件。");
                }
                timer.Stop();
                Console.WriteLine("提取结束,反序列化切片用时:" + timer.Elapsed);
                //恢复UTXO字典
                utxoDictionary = sliceFileItemObject.utxoDictionary;
                //恢复其它
                sliceFileAmount = sliceFileItemObject.sliceFileAmount;
                sameTransactionCount = sliceFileItemObject.sameTransactionCount;
                sliceFileItemObject.Dispose();
                File.Delete(utxoSlicerContextFileFinalPath);//删除解压后的文件UtxoSlice文件
                Console.WriteLine("UtxoSlice上下文状态恢复成功.........");
            }
            else
            {
                Console.WriteLine(utxoSlicerContextFileFinalPath + " 文件不存在!!!");
            }
        }

        //----8.参数检测----
        public void parameter_Detection()
        {
            bool success = true;
            if (!Directory.Exists(blockchainFilePath))
            {
                Console.WriteLine(blockchainFilePath + " 不存在!!!");
                success = false;
            }
            if (!Directory.Exists(blockProcessContextFilePath) && blockProcessContextFileName == null)
            {
                Directory.CreateDirectory(blockProcessContextFilePath);
            }
            if (!Directory.Exists(blockProcessContextFilePath) && blockProcessContextFileName != null)
            {
                Console.WriteLine(blockProcessContextFilePath + " 不存在!!!");
                success = false;
            }
            if (Directory.Exists(blockProcessContextFilePath) && blockProcessContextFileName != null)
            {
                string path = Path.Combine(blockProcessContextFilePath, blockProcessContextFileName);
                if (!File.Exists(path))
                {
                    Console.WriteLine(path + " 不存在!!!");
                    success = false;
                }
            }
            if (!Directory.Exists(UtxoSliceFilePath) && UtxoSliceFileName == null)
            {
                Directory.CreateDirectory(UtxoSliceFilePath);
            }
            if (!Directory.Exists(UtxoSliceFilePath) && UtxoSliceFileName != null)
            {
                Console.WriteLine(UtxoSliceFilePath + "不存在或错误!!!");
                success = false;
            }
            if (Directory.Exists(UtxoSliceFilePath) && UtxoSliceFileName != null)
            {
                string path = Path.Combine(UtxoSliceFilePath, UtxoSliceFileName);
                if (!File.Exists(path))
                {
                    Console.WriteLine(path + " 不存在!!!");
                    success = false;
                }
            }
            if (!Directory.Exists(OpReturnFilePath))
            {
                Directory.CreateDirectory(OpReturnFilePath);
            }
            if (sliceIntervalTimeType != "year" && sliceIntervalTimeType != "month" && sliceIntervalTimeType != "day")
            {
                Console.WriteLine("时间间隔类型参数错误(year/month/day)!!!");
                success = false;
            }
            if (sliceIntervalTime < 0)
            {
                Console.WriteLine("时间间隔不能小于0");
                success = false;
            }
            if (endTime < new DateTime(2009, 1, 3, 18, 15, 05))
            {
                Console.WriteLine("时间不能早于 2009/1/3 18:15:05!!!");
                success = false;
            }
            if (endBlockHeight < 0)
            {
                Console.WriteLine("区块高度不能小于0!!!");
                success = false;
            }
            if (success == false)
            {
                Environment.Exit(0);
            }
        }

        ////II.*****数据库初始化*****
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
        //a.分别判断关于切片的2张表是否存在，没有则创建
        public void sliceTableCreate(bool printMark)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand;
                //1.创建SliceUTXO表
                if (!tableExist("SliceUTXO"))
                {
                    string commandStr = "CREATE TABLE [dbo].[SliceUTXO]([SliceRecordID][bigint] NOT NULL PRIMARY KEY,[SliceID] [int] NOT NULL,[TxOutID] [bigint] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("SliceUTXO表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("SliceUTXO表已存在!!!");
                    }
                }
                //2.创建SliceInfo表
                if (!tableExist("SliceInfo"))
                {
                    string commandStr = "CREATE TABLE [dbo].[SliceInfo]([SliceID][int] NOT NULL PRIMARY KEY,[BlockID] [bigint] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("SliceInfo表创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("SliceInfo表已存在!!!");
                    }
                }
                sqlConnection.Close();
            }
        }

        //b.分别判断关于切片的2张表的存储过程是否存在，没有则创建
        public void sliceTableProcedureCreate(bool printMark)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand;
                //1.创建SliceInfo表的插入存储过程
                if (!procedureExist("Insert_SliceInfo_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_SliceInfo_Proc @SliceID int,@BlockID bigint AS BEGIN INSERT INTO [dbo].[SliceInfo] VALUES(@SliceID, @BlockID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_SliceInfo_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_SliceInfo_Proc已存在!!!");
                    }
                }
                //2.创建SliceInfo表的插入存储过程
                if (!procedureExist("Insert_SliceUTXO_Proc"))
                {
                    string commandStr = "CREATE PROC Insert_SliceUTXO_Proc @SliceRecordID bigint,@SliceID int,@TxOutID bigint AS BEGIN INSERT INTO [dbo].[SliceUTXO] VALUES(@SliceRecordID, @SliceID,@TxOutID) END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_SliceUTXO_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Insert_SliceUTXO_Proc已存在!!!");
                    }
                }
                //3.创建获取SliceInfo表最大SliceID的存储过程
                if (!procedureExist("Get_MaxSliceID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxSliceID_Proc @SliceID int OUTPUT AS BEGIN SELECT @SliceID = MAX(SliceID) FROM [dbo].[SliceInfo] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxSliceID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxSliceID_Proc已存在!!!");
                    }
                }
                //4.创建获取SliceUTXO表最大SliceRecordID的存储过程
                if (!procedureExist("Get_MaxSliceRecordID_Proc"))
                {
                    string commandStr = "CREATE PROC Get_MaxSliceRecordID_Proc @SliceRecordID bigint OUTPUT AS BEGIN SELECT @SliceRecordID = MAX(SliceRecordID) FROM [dbo].[SliceUTXO] END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxSliceRecordID_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Get_MaxSliceRecordID_Proc已存在!!!");
                    }
                }
                //5.创建恢复SliceUTXO表的存储过程
                if (!procedureExist("Restore_SliceUTXOTable_Proc"))
                {
                    string commandStr = "CREATE PROC Restore_SliceUTXOTable_Proc @SliceID int AS BEGIN DELETE FROM [dbo].[SliceUTXO] WHERE SliceID>@SliceID END";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_SliceUTXOTable_Proc创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("存储过程Restore_SliceUTXOTable_Proc已存在!!!");
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
                //1.创建SliceUTXOTableType表值
                if (!tableTypeExist("SliceUTXOTableType"))
                {
                    string commandStr = "CREATE TYPE SliceUTXOTableType AS TABLE([SliceRecordID][bigint] NOT NULL,[SliceID] [int] NOT NULL,[TxOutID] [bigint] NOT NULL)";
                    sqlCommand = new SqlCommand(commandStr, sqlConnection);
                    sqlCommand.ExecuteNonQuery();
                    if (printMark)
                    {
                        Console.WriteLine("SliceUTXOTableType表值创建成功!!!");
                    }
                }
                else
                {
                    if (printMark)
                    {
                        Console.WriteLine("SliceUTXOTableType表值已存在!!!");
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
                    sliceTableCreate(printMark);
                    sliceTableProcedureCreate(printMark);
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

        ////III.*****数据库恢复*****
        //----5.恢复数据库中的对应表----
        //a.获取SliceInfo表中最大的ID
        public int getMaxSliceID()
        {
            int maxSliceID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxSliceID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxSliceIDSqlPar = new SqlParameter("@SliceID", SqlDbType.BigInt);
                maxSliceIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxSliceIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@SliceID"].Value == System.DBNull.Value)
                {
                    maxSliceID = -1;
                }
                else
                {
                    maxSliceID = Convert.ToInt32(cmd.Parameters["@SliceID"].Value);
                }
                sqlConnection.Close();
            }
            return maxSliceID;
        }

        //b.获取SliceUTXO表中最大的ID
        public Int64 getMaxSliceRecordID()
        {
            Int64 maxSliceRecordID;
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Get_MaxSliceRecordID_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                SqlParameter maxSliceRecordIDSqlPar = new SqlParameter("@SliceRecordID", SqlDbType.BigInt);
                maxSliceRecordIDSqlPar.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(maxSliceRecordIDSqlPar);
                cmd.ExecuteNonQuery();
                if (cmd.Parameters["@SliceRecordID"].Value == System.DBNull.Value)
                {
                    maxSliceRecordID = -1;
                }
                else
                {
                    maxSliceRecordID = Convert.ToInt64(cmd.Parameters["@SliceRecordID"].Value);
                }
                sqlConnection.Close();
            }
            return maxSliceRecordID;
        }

        //c.恢复SliceUTXO表的完整性状态
        public void restore_SliceUTXO_Table()
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Restore_SliceUTXOTable_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@SliceID", getMaxSliceID());
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        ////IV.*****向数据库中写数据*****
        //----1.在恢复了数据库后获取nextSliceID、nextSliceRecordID----
        public void get_NextPrimaryKeyIDS(bool printMark)
        {
            nextSliceID = getMaxSliceID() + 1;
            nextSliceRecordID = getMaxSliceRecordID() + 1;
            if (printMark)
            {
                Console.WriteLine("nextSliceID:" + nextSliceID);
                Console.WriteLine("nextSliceRecordID:" + nextSliceRecordID);
            }
        }

        //----2.SliceInfo和SliceUTXO数据写库函数
        //a.保存切片数据到SliceInfo表
        public void save_SliceInfo_Table(int sliceID, Int64 blockID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Insert_SliceInfo_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@SliceID", sliceID);
                cmd.Parameters.AddWithValue("@BlockID", blockID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //b.创建SliceUTXO的数据表值类型
        public DataTable get_SliceUTXO_TableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("SliceRecordID",typeof(Int64)),//0
                new DataColumn("SliceID",typeof(int)),//1
                new DataColumn("TxOutID",typeof(Int64))//2

            });
            return dt;
        }

        //c.添加一条记录到SliceUTXO的数据表值
        public void addOneRowToSliceUTXODataTable(DataTable dataTable, Int64 sliceRecordID, int sliceID, Int64 txOutID)
        {
            DataRow dataRow = dataTable.NewRow();
            dataRow[0] = sliceRecordID;
            dataRow[1] = sliceID;
            dataRow[2] = txOutID;
            dataTable.Rows.Add(dataRow);
        }

        //d.将一个UTXO字典中的所有输出添加到SliceUTXO的数据表值
        public void addAllSliceUTXOToSliceUTXODataTable(DataTable dataTable, int sliceID)
        {
            foreach (UTXOItem_Class utxoItem in utxoDictionary.Values)
            {
                for (int i = 0; i < utxoItem.utxoItemAmount; i++)
                {
                    addOneRowToSliceUTXODataTable(dataTable, nextSliceRecordID, sliceID, utxoItem.TxOutID[i]);
                    nextSliceRecordID++;
                }
            }
        }

        //e.将SliceUTXO表值中的数据写入到数据库中
        public void sliceUTXOTableValuedToDB(DataTable dataTable)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                string TSqlStatement = "INSERT INTO[dbo].[SliceUTXO] (SliceRecordID, SliceID,TxOutID)" +
                                        "SELECT nc.SliceRecordID,nc.SliceID,nc.TxOutID FROM @NewBulkTestTvp AS nc";
                SqlCommand cmd = new SqlCommand(TSqlStatement, sqlConnection);
                SqlParameter catParam = cmd.Parameters.AddWithValue("@NewBulkTestTvp", dataTable);
                catParam.SqlDbType = SqlDbType.Structured;
                catParam.TypeName = "[dbo].[SliceUTXOTableType]";
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

        //f.将两个表值中的数据全都写入到数据库中
        public void allSliceDataToDB(DataTable sliceUTXODataTable, Int64 blockID)
        {
            Console.WriteLine("正在将第" + sliceFileAmount + "个切片数据写入数据库，请勿现在终止程序..........");
            addAllSliceUTXOToSliceUTXODataTable(sliceUTXODataTable,nextSliceID);
            sliceUTXOTableValuedToDB(sliceUTXODataTable);
            save_SliceInfo_Table(nextSliceID, blockID);
            nextSliceID++;
        }

        ////V.****本项目中不使用的函数****
        //1.更新UTXO状态(***本项目中不使用***)
        public void updateUTXO()
        {
            if (orderedBlockchainParser.processedBlockAmount == 0)
            {
                Console.WriteLine("初次启动.........");
            }
            else
            {
                Console.WriteLine("从中断处恢复后启动.........");
            }
            ParserBlock readyBlock;
            while ((readyBlock = orderedBlockchainParser.getNextBlock()) != null)
            {
                execute_TransactionsOfOneBlock(readyBlock);
                if (endConditionJudgment(recentlySliceDateTime, readyBlock.Header.BlockTime.DateTime))
                {
                    sliceFileAmount++;
                    Console.WriteLine("正在保存第" + sliceFileAmount + "个切片状态,请勿现在终止程序..........");
                    save_SliceFile(orderedBlockchainParser.processedBlockAmount, sliceFileAmount, readyBlock.Header.BlockTime.DateTime);
                    Console.WriteLine("UTXO切片保存完成");
                    Console.WriteLine("正在保存第" + sliceFileAmount + "个程序上下文状态,请勿现在终止程序..........");
                    orderedBlockchainParser.saveBlockProcessContext();
                    Console.WriteLine("程序上下文保存完成");
                    if (OpReturnFilePath != null)
                    {
                        Console.WriteLine("正在保存第" + sliceFileAmount + "个opreturn切片状态,请勿现在终止程序..........");
                        save_opreturnOutputsFile(orderedBlockchainParser.processedBlockAmount, readyBlock.Header.BlockTime.DateTime);
                        Console.WriteLine("opreturn切片保存完成");
                    }
                    recentlySliceDateTime = readyBlock.Header.BlockTime.DateTime;
                }
                if (orderedBlockchainParser.processedBlockAmount % 100 == 0)
                {
                    Console.WriteLine("已处理" + orderedBlockchainParser.processedBlockAmount + "个区块");
                    Console.WriteLine("当前区块时间:" + readyBlock.Header.BlockTime.DateTime);
                    Console.WriteLine("相同交易出现次数:" + sameTransactionCount);
                }
                if (readyBlock.Header.BlockTime.DateTime >= endTime)
                {
                    Console.WriteLine("当前区块时间:" + readyBlock.Header.BlockTime.DateTime);
                    Console.WriteLine("当前区块高度:" + (orderedBlockchainParser.processedBlockAmount - 1));
                    Console.WriteLine("触发时间终止条件，执行结束!!!");
                    break;
                }
                if (orderedBlockchainParser.processedBlockAmount - 1 >= endBlockHeight)
                {
                    Console.WriteLine("当前区块时间:" + readyBlock.Header.BlockTime.DateTime);
                    Console.WriteLine("当前区块高度:" + (orderedBlockchainParser.processedBlockAmount - 1));
                    Console.WriteLine("触发区块高度终止条件，执行结束!!!");
                    break;
                }
            }
        }

        //2.执行一个区块的交易(***本项目中不使用***)
        public void execute_TransactionsOfOneBlock(ParserBlock block)
        {
            foreach (Transaction transaction in block.Transactions)
            {
                if (transaction.IsCoinBase)
                {
                    execute_CoinbaseTransaction(transaction);
                }
                else
                {
                    if (isValidTransaction(transaction))
                    {
                        execute_RegularTransaction(transaction);
                    }
                }
            }
        }

        //3.保存切片数据到SliceInfo表(***本项目中不使用***)
        public void save_SliceUTXO_Table(Int64 sliceRecordID, int sliceID, Int64 txOutID)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = sqlConnection;
                cmd.CommandText = "Insert_SliceUTXO_Proc";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@SliceRecordID", sliceRecordID);
                cmd.Parameters.AddWithValue("@SliceID", sliceID);
                cmd.Parameters.AddWithValue("@TxOutID", txOutID);
                cmd.ExecuteNonQuery();
                sqlConnection.Close();
            }
        }

        //4.保存一个切片的所有SliceUTXO表(***本项目中不使用***)
        public void save_AllSliceUTXOTableOfOneSlice(int sliceID)
        {
            foreach (UTXOItem_Class utxoItem in utxoDictionary.Values)
            {
                for (int i = 0; i < utxoItem.utxoItemAmount; i++)
                {
                    save_SliceUTXO_Table(nextSliceRecordID, sliceID, utxoItem.TxOutID[i]);
                    nextSliceRecordID++;
                }
            }
        }

        //5.保存一个切片相关的所有表(SliceUTXO表和SliceInfo表)(***本项目中不使用***)
        public void save_AllTableOfOneSlice(Int64 blockID)
        {
            Console.WriteLine("正在将第" + sliceFileAmount + "个切片数据写入数据库，请勿现在终止程序..........");
            save_AllSliceUTXOTableOfOneSlice(nextSliceID);
            save_SliceInfo_Table(nextSliceID, blockID);
            nextSliceID++;
        }
    }
}
