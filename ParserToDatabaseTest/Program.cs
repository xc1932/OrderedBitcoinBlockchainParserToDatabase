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

namespace ParserToDatabaseTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string sqlConnectionString = "Data Source=DESKTOP-0B83G22\\SQL2016;Initial Catalog=Bitcoin;Integrated Security=True";
            ////1.初次启动
            //OrderedBitcoinBlockchainParserToDatabase_Class orderedBitcoinBlockchainParserToDatabase = new OrderedBitcoinBlockchainParserToDatabase_Class(@"F:\data\blocks", @"F:\writedatabase\blockProcessContextFileForDatabase",
            //null, @"F:\writedatabase\sliceStateFileForDatabase", null, @"F:\writedatabase\opreturnOutputFileForDatabase",Configuration_Class.Month, 3, new DateTime(2019, 5, 3), 681572, sqlConnectionString);

            //2.增量处理
            OrderedBitcoinBlockchainParserToDatabase_Class orderedBitcoinBlockchainParserToDatabase = new OrderedBitcoinBlockchainParserToDatabase_Class(@"F:\data\blocks", @"F:\writedatabase\blockProcessContextFileForDatabase",
            "BPC_132644_2011年06月22日20时25分08秒.dat.rar", @"F:\writedatabase\sliceStateFileForDatabase", "UtxoSlice_132644_2011年06月22日20时25分08秒.dat.rar", @"F:\writedatabase\opreturnOutputFileForDatabase",
            Configuration_Class.Month, 3, new DateTime(2019, 5, 3), 681572, sqlConnectionString);

            orderedBitcoinBlockchainParserToDatabase.run();
        }
    }
}
