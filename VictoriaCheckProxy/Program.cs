using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.ComponentModel;
using System.Globalization;
using System.IO.Pipelines;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Unicode;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace VictoriaCheckProxy
{
    

    public class Program
    {
        public static long startDate;
        public static long endDate;
        public static string storageEP;
        public static int compressLevel=0;
        public static int connectionLimit = 5;
        internal static VMStorageConnectionPool connectionPool = null;
        public static ILoggerFactory factory;

        public static async Task Main(string[] args)
        {
            if (args.Length == 0)
                throw new Exception("2 args expected");
            var storedMonth = args[0];
            storageEP = args[1];
            if (args.Length > 2) {
                compressLevel = int.Parse(args[2]);
            }
            if (args.Length > 3)
            {
                connectionLimit = int.Parse(args[3]);
            }
            factory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Error));
            DateTimeOffset date = DateTime.ParseExact(storedMonth, "yyyy-MM", CultureInfo.InvariantCulture);
            var logger = factory.CreateLogger("Main");
            startDate = date.ToUnixTimeMilliseconds();
            
            connectionPool = new VMStorageConnectionPool(connectionLimit);
            //DateTime.Now.Date.AddDays(-DateTime.Now.Day + 1);
            endDate = date.AddMonths(1).ToUnixTimeMilliseconds();
            logger.LogInformation("starting reception");
            var server = new TcpListener(IPAddress.Any, 8801);
            server.Start();
            logger.LogInformation("started reception");
            while (true)
            {
                var client = await server.AcceptTcpClientAsync();
                var cw = new ClientWorking(client, true);
                new Thread(cw.ClientWorker).Start();
            }
        }

    }

    internal class ClientWorking
    {
        //const string vminsertHello = "vminsert.02";
        internal const string vmselectHello = "vmselect.01";
        internal const string successResponse = "ok";
        //const string searchMethod = "search_v7";
        internal const bool isCompressed = true;

        private static readonly byte[] emptyResponse = new byte[24];

        TcpClient _client;
        bool _ownsClient;

        public ClientWorking(TcpClient client, bool ownsClient)
        {
            _client = client;
            _ownsClient = ownsClient;
        }

        public async void ClientWorker()
        {
            var logger = Program.factory.CreateLogger("ClientWorker");
            try
            {
                //Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} connection opened from {_client.Client.RemoteEndPoint.ToString()}");
                using var stream = _client.GetStream();
                using var clientCompressor = new CompressionStream(stream, level: Program.compressLevel);
                ///Handshake begin 
                try
                {
                    Helpers.GetMessage(vmselectHello, stream);
                    Helpers.SendMessage(successResponse, stream);
                    byte isRemoteCompressed = (byte)stream.ReadByte();
                    Helpers.SendMessage(successResponse, stream);
                    stream.WriteByte(1);
                    Helpers.GetMessage(successResponse, stream);
                }
                catch (Exception ex)
                {
                    var tmp = ArrayPool<byte>.Shared.Rent(256);
                    var got = stream.ReadAtLeast(tmp, 8);
                    logger.LogError(ex, $"Unread {got} bytes {BitConverter.ToString(tmp)} ");
                    throw;
                }
                ///Handshake end 
                //var pipe = new Pipe();
                //var decomp = new DecompressionStream(pipe.Reader.AsStream());
                byte[] pad = new byte[6];
                byte[] commonPart = new byte[5];
                byte[] headPart = new byte[8];
                
                while (_client.Connected)
                {
                    var rejectedMethod = false;
                    byte[] postfix = Array.Empty<byte>();
                    byte[] prefix = Array.Empty<byte>();
                    stream.ReadExactly(pad);

                    string method = Converter.UnmarshalString(stream);
                    
                    //bool bypass = false;
                    stream.ReadExactly(commonPart); //tracing flag + timeout

                    long packetSize = 0;
                    switch (method)
                    {
                        case "labelValues_v5":
                            prefix = Converter.ReadLongString(stream);
                            stream.ReadExactly(headPart);
                            packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart));

                            //tagName = Converter.UnmarshalString(sr);
                            //shift += tagName.Length + 2;
                            break;
                        case "searchMetricNames_v3":
                        case "labelNames_v5":
                        case "search_v7":
                        case "tsdbStatus_v5":
                        //case "tenants_v1":
                            stream.ReadExactly(headPart);
                            packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart));
                            break;
                        case "tenants_v1":
                            packetSize = 16;
                            break;
                        default:
                            //bypass = true;
                            logger.LogError("Pad: " + BitConverter.ToString(pad) + "\r\n" + "Common: " + BitConverter.ToString(commonPart));
                            //_client.Client.Shutdown(SocketShutdown.Both);
                            throw new Exception($"{Thread.CurrentThread.ManagedThreadId} unsupported method: {method}");
                            
                    }
                    logger.LogDebug($"Method: {method} with packet size {packetSize}");
                    //bool traceEnabled = sr.ReadBoolean();
                    //uint timeout = BinaryPrimitives.ReverseEndianness(sr.ReadUInt32());
                    //long packetSize = BinaryPrimitives.ReverseEndianness(sr.ReadInt64());
                    //long packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart, 5)); 
                    var packet = ArrayPool<byte>.Shared.Rent((int)packetSize);
                    stream.ReadExactly(packet, 0, (int)packetSize);
                    switch (method)
                    {
                        case "labelNames_v5":
                        case "labelValues_v5":
                            postfix = new byte[4];//ArrayPool<byte>.Shared.Rent(4);
                            stream.ReadExactly(postfix);
                            break;
                        case "tsdbStatus_v5":
                            postfix = Converter.ReadLongString(stream);
                            var topN = Converter.UnmarshalUint32(stream);
                            rejectedMethod = true;
                            break;

                    }
                    int lastPos = 0;
                    uint accountId = 0;
                    uint projectId = 0;
                    if (method != "tenants_v1")
                    {
                        accountId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 0));
                        projectId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 4));
                        lastPos = 8;
                    }
                    
                    long minTs = long.MinValue;
                    long maxTs = long.MaxValue;
                    lastPos += Converter.UnmarshalVarInt64(packet, ref minTs, lastPos);
                    lastPos += Converter.UnmarshalVarInt64(packet, ref maxTs, lastPos);
                    logger.LogDebug($"From query got tenant {accountId}:{projectId} from {minTs} to {maxTs}");
                    if (minTs < Program.endDate && Program.startDate < maxTs && !rejectedMethod)
                    {
                        
                        VMStorageConnection vmstorageConn =  Program.connectionPool.GetClient();
                        var buffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);
                        try
                        {
                            var cts = new CancellationTokenSource();
                            vmstorageConn.networkStream.Write(pad);
                            vmstorageConn.networkStream.Write(Converter.MarshalString(method));
                            vmstorageConn.networkStream.Write(commonPart);
                            if (prefix.Length > 0)
                            {
                                vmstorageConn.networkStream.Write(prefix);
                                //ArrayPool<byte>.Shared.Return(prefix);
                            }
                            vmstorageConn.networkStream.Write(headPart);
                            vmstorageConn.networkStream.Write(packet, 0, (int)packetSize);
                            if (postfix.Length > 0)
                            {
                                vmstorageConn.networkStream.Write(postfix);
                                //ArrayPool<byte>.Shared.Return(postfix);
                            }
                            vmstorageConn.networkStream.Flush();
                            logger.LogDebug($"Sent query to vmstorage");
                            int bytesRead = 0;

                            int totalRead = 0;
                            
                            int currPos = 0;
                            int blockCount = 0;
                            ulong blockSize = 0;
                            
                            try
                            {
                                var errorMessage = Converter.ReadLongString(vmstorageConn.decompressor);
                                clientCompressor.Write(errorMessage);
                                //ArrayPool<byte>.Shared.Return(errorMessage);
                                blockSize = Converter.UnmarshalUint64(vmstorageConn.decompressor);
                                logger.LogDebug($"First block size {blockSize}");
                                clientCompressor.Write(Converter.MarshalUint64(blockSize));
                                blockCount = 1;
                                while (blockSize > 0) {
                                    if (blockSize > (ulong) buffer.Length)
                                    {
                                        vmstorageConn.decompressor.ReadExactly(buffer);
                                        blockSize -= (ulong)buffer.Length;
                                        bytesRead = buffer.Length;
                                        logger.LogDebug("Block size too large for one read");
                                    }
                                    else
                                    {
                                        vmstorageConn.decompressor.ReadExactly(buffer, 0, (int)blockSize);
                                        bytesRead = (int)blockSize;
                                        blockSize = 0;   
                                    }
                                    
                                    clientCompressor.Write(buffer, 0, bytesRead);
                                    
                                    if (blockSize == 0) {
                                        blockSize = Converter.UnmarshalUint64(vmstorageConn.decompressor);
                                        clientCompressor.Write(Converter.MarshalUint64(blockSize));
                                        logger.LogDebug($"New block size {blockSize}");
                                    }
                                    /*if (blockSize == 0)
                                    {
                                        Console.WriteLine("new block is empty!");
                                    }*/
                                }
                                var complete = Converter.ReadLongString(vmstorageConn.decompressor);
                                clientCompressor.Write(complete);
                                //ArrayPool<byte>.Shared.Return(errorMessage);
                                logger.LogDebug("End reading of response from vmstorage");
                                clientCompressor.Flush();
                            }

                            catch (OperationCanceledException) { }
                            catch (SocketException) { }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "");
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(buffer);
                            Program.connectionPool.ReturnClient(vmstorageConn);
                        }
                    }
                    else
                    {
                        clientCompressor.Write(emptyResponse);
                        switch (method)
                        {
                            case "tsdbStatus_v5":
                                clientCompressor.Write(emptyResponse);
                                clientCompressor.Write(emptyResponse);
                                break;
                            //case "tenants_v1"
                        }
                        clientCompressor.Flush();
                    }
                    ArrayPool<byte>.Shared.Return(packet);
                    //stream.Flush();
                    //vmstorStream.Flush();
                    //_client.Close();

                }
            }
            catch (EndOfStreamException) { } //хпуой
            catch (Exception ex)
            {
                logger.LogError(ex, "");
            }
            finally
            {
                
                if (_ownsClient && _client != null)
                {
                    logger.LogInformation("Connection closed");
                    (_client as IDisposable).Dispose();
                    _client = null;
                }
            }
        }

        
    }
}
