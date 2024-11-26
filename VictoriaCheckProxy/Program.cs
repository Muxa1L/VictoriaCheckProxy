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
            DateTimeOffset date = DateTime.ParseExact(storedMonth, "yyyy-MM", CultureInfo.InvariantCulture);
            startDate = date.ToUnixTimeMilliseconds();
            
            connectionPool = new VMStorageConnectionPool(connectionLimit);
            //DateTime.Now.Date.AddDays(-DateTime.Now.Day + 1);
            endDate = date.AddMonths(1).ToUnixTimeMilliseconds();
            await MainAsync();
        }

        

        static async Task MainAsync()
        {
            Console.WriteLine("Starting...");
            var server = new TcpListener(IPAddress.Any, 8801);
            server.Start();
            Console.WriteLine("Started.");
            while (true)
            {
                var client = await server.AcceptTcpClientAsync();
                var cw = new ClientWorking(client, true);
                new Thread(cw.DoSomethingWithClientAsync).Start();
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

        public async void DoSomethingWithClientAsync()
        {
            
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
                    Console.WriteLine(ex.ToString() );
                    var tmp = ArrayPool<byte>.Shared.Rent(256);
                    
                    var got = stream.ReadAtLeast(tmp, 8);
                    Console.WriteLine($"Unread {got} bytes {BitConverter.ToString(tmp)} ");
                    throw;
                }                
                ///Handshake end 
                //var pipe = new Pipe();
                //var decomp = new DecompressionStream(pipe.Reader.AsStream());
                while (_client.Connected)
                {
                    var rejectedMethod = false;
                    byte[] pad = new byte[6];
                    stream.ReadExactly(pad);

                    string method = Converter.UnmarshalString(stream);
                    //bool bypass = false;
                    byte[] commonPart = new byte[5];
                    stream.ReadExactly(commonPart); //tracing flag + timeout
                    byte[] prefix = Array.Empty<byte>();
                    byte[] headPart = new byte[8];
                    byte[] postfix = Array.Empty<byte>();

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
                            Console.WriteLine("Pad: " + BitConverter.ToString(pad));
                            Console.WriteLine("Common: " + BitConverter.ToString(commonPart));
                            //_client.Client.Shutdown(SocketShutdown.Both);
                            throw new Exception($"{Thread.CurrentThread.ManagedThreadId} unsupported method: {method}");
                            
                    }

                    //bool traceEnabled = sr.ReadBoolean();
                    //uint timeout = BinaryPrimitives.ReverseEndianness(sr.ReadUInt32());
                    //long packetSize = BinaryPrimitives.ReverseEndianness(sr.ReadInt64());
                    //long packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart, 5)); 
                    var packet = new byte[packetSize];
                    stream.ReadExactly(packet);
                    switch (method)
                    {
                        case "labelNames_v5":
                        case "labelValues_v5":
                            postfix = new byte[4];
                            stream.ReadExactly(postfix);
                            break;
                        case "tsdbStatus_v5":
                            postfix = Converter.ReadLongString(stream);
                            var topN = Converter.UnmarshalUint32(stream);
                            rejectedMethod = true;
                            break;

                    }
                    int lastPos = 0;
                    if (method != "tenants_v1")
                    {
                        var accountId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 0));
                        var projectId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 4));
                        lastPos = 8;
                    }
                    
                    long minTs = long.MinValue;
                    long maxTs = long.MaxValue;
                    lastPos += Converter.UnmarshalVarInt64(packet, ref minTs, lastPos);
                    lastPos += Converter.UnmarshalVarInt64(packet, ref maxTs, lastPos);
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
                            }
                            vmstorageConn.networkStream.Write(headPart);
                            vmstorageConn.networkStream.Write(packet);
                            if (postfix.Length > 0)
                            {
                                vmstorageConn.networkStream.Write(postfix);
                            }
                            vmstorageConn.networkStream.Flush();
                            
                            int bytesRead = 0;

                            int totalRead = 0;
                            bool isCompleted = false;
                            bool startMarkerRead = false;
                            
                            int currPos = 0;
                            int blockCount = 0;
                            ulong blockSize = 0;
                            
                            try
                            {
                                while (!isCompleted)
                                {
                                    if (currPos < 0)
                                    {
                                        currPos = Math.Abs(currPos);
                                        Array.Copy(buffer, bytesRead - currPos, buffer, 0, currPos);
                                        bytesRead = vmstorageConn.decompressor.Read(buffer, currPos, buffer.Length - currPos);
                                        bytesRead += currPos;
                                        currPos = 0;
                                    }
                                    else
                                    {
                                        bytesRead = vmstorageConn.decompressor.Read(buffer);
                                    }

                                    totalRead += bytesRead;
                                    if (!startMarkerRead)
                                    {
                                        var empty = Converter.UnmarshalString(buffer);
                                        if (empty != "")
                                        {
                                            throw new Exception("kaka" + empty);
                                        }
                                        else
                                        {
                                            currPos = 8;
                                            startMarkerRead = true;
                                        }
                                        blockSize = Converter.UnmarshalUint64(buffer, 8);
                                        blockCount = 1;
                                        currPos += 8 + (int)blockSize;
                                    }

                                    while (currPos < bytesRead - 8 && blockSize != 0)
                                    {
                                        blockSize = Converter.UnmarshalUint64(buffer, currPos);
                                        currPos = currPos + 8 + (int)blockSize;
                                        blockCount++;
                                    }
                                    if (blockSize == 0)
                                    {
                                        //Console.WriteLine("Empty block");
                                    }
                                    if (bytesRead - currPos == 8)
                                    {
                                        try
                                        {
                                            var complete = Converter.UnmarshalLongString(buffer, bytesRead - 8);
                                            if (complete == "")
                                            {
                                                isCompleted = true;
                                            }
                                        }
                                        catch (Exception) { }
                                    }
                                    else
                                    {
                                        currPos = currPos - bytesRead;
                                    }
                                    clientCompressor.Write(buffer, 0, bytesRead);
                                    clientCompressor.Flush();
                                    if (!cts.IsCancellationRequested)
                                    {

                                    }

                                }
                            }

                            catch (OperationCanceledException) { }
                            catch (SocketException) { }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("302-" + ex.ToString() + "\r\n" + ex.StackTrace);
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

                    //stream.Flush();
                    //vmstorStream.Flush();
                    //_client.Close();

                }
            }
            catch (EndOfStreamException) { } //хпуой
            catch (Exception ex)
            {
                Console.WriteLine("330-"+ex.ToString() + "\r\n" + ex.StackTrace);
            }
            finally
            {
                
                if (_ownsClient && _client != null)
                {
                    //Console.WriteLine("connection closed");
                    (_client as IDisposable).Dispose();
                    _client = null;
                }
            }
        }

        
    }
}
