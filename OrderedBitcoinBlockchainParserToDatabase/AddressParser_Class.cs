using System;
using System.Collections.Generic;
using System.Text;
using System.Numerics;
using System.Security.Cryptography;

namespace OrderedBitcoinBlockchainParserToDatabase
{
    public class AddressParser_Class
    {
        const int publickeyLength = 65;
        const int compressedPublickeyLength = 33;
        const int hashLength = 20;//脚本hash长度
        const int hashWithChecksumLength = 25;
        internal const byte mainnet = 0x00;
        const byte payToHash = 0x05;

        //1.从输出脚本中提取出地址
        public string extractAddressFromScript(byte[] script, byte netType, out bool isNonStandardPayment)
        {
            string address = null;
            isNonStandardPayment = false;
            if (script != null)
            {
                byte[] publickeyHash;
                if (ispayToPublickeyHash(script, out publickeyHash))//4.支付到公钥hash
                {
                    address = publickeyHashToAddress(publickeyHash, netType);//5.公钥hash到地址
                }
                else
                {
                    byte[] publickey;
                    if (ispayToPublickey(script, out publickey))//8.支付到公钥
                    {
                        address = publickeyToAddress(publickey, netType);//9.公钥到地址
                    }
                    else
                    {
                        byte[] scriptHash;
                        if (ispayToScriptHash(script, out scriptHash))//12.支付到脚本hash
                        {
                            address = scriptHashToAddress(scriptHash);//13.脚本hash到地址
                        }
                        else
                        {
                            if (ispayToMultisig(script))//15.多签名支付
                            {
                                address = multisigToAddress(script);//16.多签名到地址
                            }
                            else
                            {
                                address = nonstandardScriptToAddress(script);//17.非标准脚本到地址
                                isNonStandardPayment = true;
                            }
                        }
                    }
                }
            }
            return (address);
        }

        //2.判断给定脚本是否是支付到公钥的方式，out参数返回对应公钥
        bool ispayToPublickey(byte[] script, out byte[] publickey)
        {
            //0x41 04E5F0F151EB1800D1B787437EEA66493C8C2C6DA2BA4D4709E0583DAEFDF5AAD   67bytes   0x41=65=pklength   0xAC  OP_CHECKSIG
            //  0E30BAE87C7F33B04B0F9E5F2045853BC45BE1526A31A264E1C17E75DA91C18E6 AC   [0]...[66]          [0]...[34]
            //  04E5F0F...........75DA91C18E6  publickey                                      pklength+1          pklength+1
            //const int publickeylength = 65;   0x41
            //const int compressedpublickeylength = 33; 0x21
            if (script.Length >= compressedPublickeyLength + 2)
            {
                int pkLength = script[0];
                if (pkLength == publickeyLength || pkLength == compressedPublickeyLength)
                {
                    if (script[pkLength + 1] == 0xac)
                    {
                        publickey = new byte[pkLength];
                        Array.Copy(script, 1, publickey, 0, pkLength);
                        return (true);
                    }
                }
            }
            publickey = null;
            return (false);
        }

        //3.判断给定脚本是否是支付到公钥hash的的方式，out参数返回对应公钥hash
        bool ispayToPublickeyHash(byte[] script, out byte[] publickeyHash)
        {
            //解析：输出脚本格式
            //hashlength=20
            //0x76 A9 14 12AB8DC588CA9D5787DDE7EB29569DA63C3A238C 88 AC  256bits
            //  0  1  2  3 4 5 6 7 8 9 101 2 3 4 5 6 7 8 9 201 2  3  4
            //0x76 OP_DUP       0xA9 OP_HASH160     0x88 OP_EQUALVERIFY      0xAC OP_CHECKSIG
            //12AB8DC588CA9D5787DDE7EB29569DA63C3A238C  <pubkeyHash>   160bits ripemd160
            //OP_DUP OP_HASH160 <pubkeyHash> OP_EQUALVERIFY OP_CHECKSIG
            int scriptLength = script.Length;
            if (scriptLength >= hashLength + 5)
            {
                if (script[0] == 0x76 && script[1] == 0xa9 && script[2] == hashLength && script[hashLength + 3] == 0x88 && script[hashLength + 4] == 0xac)
                {
                    publickeyHash = new byte[hashLength];
                    Array.Copy(script, 3, publickeyHash, 0, hashLength);
                    return (true);
                }
            }
            publickeyHash = null;
            return (false);
        }

        //4.判断给定脚本是否是支付到脚本hash的方式，out参数返回对应脚本hash
        bool ispayToScriptHash(byte[] script, out byte[] scriptHash)
        {
            //const int hashlength = 20;  //脚本hash 20bytes 可能存的是数据
            //0xA9 OP_HASH160  0x14 hashlength  0x87 OP_EQUAL
            //[0]        [1]        [2]...[21]    [22]
            //OP_HASH160 hashlength  scripthash    OP_EQUAL
            if (script.Length >= 3 + hashLength)
            {
                if (script[0] == 0xa9 && script[1] == hashLength && script[hashLength + 2] == 0x87)
                {
                    scriptHash = new byte[hashLength];
                    Array.Copy(script, 2, scriptHash, 0, hashLength);
                    return (true);
                }
            }
            scriptHash = null;
            return (false);
        }

        //5.判断给定的脚本是否多签名支付方式
        bool ispayToMultisig(byte[] script)
        {
            //m <pubkey 1>...<pubkey n> n  OP_CHECKMULTISIG
            //0xae  OP_CHECKMULTISIG
            int scriptLength = script.Length;
            if (script.Length >= 3 + compressedPublickeyLength)
            {
                if (script[scriptLength - 1] == 0xae)
                {
                    return (true);
                }
            }
            return (false);
        }

        //6.根据公钥和网络类型返回对应地址
        string publickeyToAddress(byte[] publickey, byte netType)
        {
            if (publickey != null)
            {                                  //10.先对公钥进行SHA256,再ripemd160
                return (publickeyHashToAddress(sha256ripemd160(publickey), netType));//11.公钥hash到地址(第二次使用)
            }
            return (null);
        }

        //7.根据公钥hash和网络类型返回对应地址
        string publickeyHashToAddress(byte[] publickeyHash, byte netType)
        {
            if (publickeyHash != null)
            {
                return (hashToAddress(publickeyHash, netType));//6.公钥hash到地址
            }
            return (null);
        }

        //8.给定脚本hash返回对应地址
        string scriptHashToAddress(byte[] scriptHash)
        {
            if (scriptHash != null)
            {
                return (hashToAddress(scriptHash, payToHash));//14.脚本hash到地址（第三次使用）
            }
            return (null);
        }

        //9.返回给定多签名脚本的地址
        string multisigToAddress(byte[] script)
        {
            if (script != null)
            {
                return (hashToAddress(sha256ripemd160(script), payToHash));
            }
            return (null);
        }

        //10.非标准脚本到地址
        string nonstandardScriptToAddress(byte[] script)
        {
            //将非标准脚本归类到多签名脚本
            return (multisigToAddress(script));
        }

        //11.先对公钥进行SHA256,再ripemd160
        byte[] sha256ripemd160(byte[] bytes)
        {
            byte[] temp = SHA256.Create().ComputeHash(bytes);
            RIPEMD160 myRIPEMD160 = RIPEMD160Managed.Create();
            return RIPEMD160.Create().ComputeHash(temp);
        }

        //12.由公钥hash到地址 区块链学习28页
        string hashToAddress(byte[] hashByte, byte netType)
        {
            int pkhlength = hashByte.Length;
            byte[] address = new byte[pkhlength + 5];
            address[0] = netType;
            Array.Copy(hashByte, 0, address, 1, pkhlength);
            byte[] temp = new byte[1 + pkhlength];
            temp[0] = netType;
            Array.Copy(hashByte, 0, temp, 1, pkhlength);
            byte[] temp2 = System.Security.Cryptography.SHA256.Create().ComputeHash(System.Security.Cryptography.SHA256.Create().ComputeHash(temp));
            Array.Copy(temp2, 0, address, pkhlength + 1, 4);
            return (Base58Encode(address));
        }

        //13.将byte[]数组进行Base58编码
        static string Base58Encode(byte[] data)
        {
            const string DIGITS = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
            BigInteger intData = 0;
            for (int i = 0; i < data.Length; i++)
            {
                intData = intData * 256 + data[i];
            }
            // Encode BigInteger to Base58 string 
            var result = string.Empty;
            while (intData > 0)
            {
                var remainder = (int)(intData % 58);
                intData /= 58;
                result = DIGITS[remainder] + result;
            }
            // Append `1` for each leading 0 byte 
            for (var i = 0; i < data.Length && data[i] == 0; i++)
            {
                result = '1' + result;
            }
            return result;
        }

        //测试地址提取功能
        public void extractAddressFromScript_Test()
        {
            Console.WriteLine("开始");
            string scriptStr = "4104678AFDB0FE5548271967F1A67130B7105CD6A828E03909A67962E0EA1F61DEB649F6BC3F4CEF38C4F35504E51EC112DE5C384DF7BA0B8D578A4C702B6BF11D5FAC";
            //                  4104E5F0F151EB1800D1B787437EEA66493C8C2C6DA2BA4D4709E0583DAEFDF5AAD0E30BAE87C7F33B04B0F9E5F2045853BC45BE1526A31A264E1C17E75DA91C18E6AC
            bool isNonStandardPayment;
            byte[] b = Org.BouncyCastle.Utilities.Encoders.Hex.Decode(scriptStr);
            string str = extractAddressFromScript(b, mainnet, out isNonStandardPayment);
            Console.WriteLine(str);
            Console.WriteLine(isNonStandardPayment);

            string scriptStr1 = "76A914BCDF02D3FAEC4DE5B15D7418F6FFCD692AE95E0888AC";
            byte[] b1 = Org.BouncyCastle.Utilities.Encoders.Hex.Decode(scriptStr1);
            string str1 = extractAddressFromScript(b1, mainnet, out isNonStandardPayment);
            Console.WriteLine(str1);
            Console.WriteLine(isNonStandardPayment);
        }
    }
}
