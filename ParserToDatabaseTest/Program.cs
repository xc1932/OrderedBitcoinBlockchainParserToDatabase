using System;
using System.Data;
using OrderedBitcoinBlockchainParserToDatabase;
using OrderedBitcoinBlockchainParser;
using BitcoinUTXOSlicer;
using BitcoinBlockchain.Parser;
using System.Linq;
using System.Collections.Generic;
using BitcoinBlockchain.Data;
using System.Diagnostics;
using NBitcoin;

namespace ParserToDatabaseTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string sqlConnectionString = "Data Source=DESKTOP-BSV;Initial Catalog=BitcoinZJ;Integrated Security=True";
            //1.初次启动
            //OrderedBitcoinBlockchainParserToDatabase_Class orderedBitcoinBlockchainParserToDatabase = new OrderedBitcoinBlockchainParserToDatabase_Class(@"F:\data\blocks", @"G:\writedatabase\blockProcessContextFileForDatabase",
            //null, @"G:\writedatabase\sliceStateFileForDatabase", null, @"G:\writedatabase\opreturnOutputFileForDatabase", Configuration_Class.Month, 3, new DateTime(2019, 5, 3), 681572, sqlConnectionString);

            //2.增量处理
            //OrderedBitcoinBlockchainParserToDatabase_Class orderedBitcoinBlockchainParserToDatabase = new OrderedBitcoinBlockchainParserToDatabase_Class(@"F:\data\blocks", @"G:\writedatabase\blockProcessContextFileForDatabase",
            //"BPC_655077_2020年11月02日04时06分58秒.dat.rar", @"G:\writedatabase\sliceStateFileForDatabase", "UtxoSlice_655077_2020年11月02日04时06分58秒.dat.rar", @"G:\writedatabase\opreturnOutputFileForDatabase",
            //Configuration_Class.Month, 3, new DateTime(2021, 12, 12), 681572, sqlConnectionString);

            //orderedBitcoinBlockchainParserToDatabase.run();            

        }
    }
}
