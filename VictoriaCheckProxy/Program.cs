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
        internal static VMStorageConnectionPool connectionPool = null;
        public static async Task Main(string[] args)
        {
            if (args.Length == 0)
                throw new Exception("2 args expected");
            var storedMonth = args[0];
            storageEP = args[1];
            DateTimeOffset date = DateTime.ParseExact(storedMonth, "yyyy-MM", CultureInfo.InvariantCulture);
            startDate = date.ToUnixTimeMilliseconds();
            connectionPool = new VMStorageConnectionPool(10);
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
                Task.Run((Func<Task>)cw.DoSomethingWithClientAsync);
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

        private static readonly byte[] zstdMagicBytes = new byte[] { 0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x60 };
        private static readonly byte[] emptyResponse = new byte[] { 0x44, 0x00, 0x00, 0x08, 0x00, 0x01, 0x54, 0x01, 0x02, 0x14, 0x04 };

        TcpClient _client;
        bool _ownsClient;

        public ClientWorking(TcpClient client, bool ownsClient)
        {
            _client = client;
            _ownsClient = ownsClient;
        }

        public async Task DoSomethingWithClientAsync()
        {
            
            try
            {
                Console.WriteLine($"connection opened from {_client.Client.RemoteEndPoint.ToString()}");
                using var stream = _client.GetStream();
                bool zstdMBSent = false;
                NetworkStream vmstorStream = null;
                ///Handshake begin 
                Helpers.GetMessage(vmselectHello, stream, true);
                Helpers.SendMessage(successResponse, stream, true);
                byte isRemoteCompressed = (byte)stream.ReadByte();
                Helpers.SendMessage(successResponse, stream, true);
                stream.WriteByte(1);
                Helpers.GetMessage(successResponse, stream, true);
                ///Handshake end 
                var pipe = new Pipe();
                var decomp = new DecompressionStream(pipe.Reader.AsStream());
                while (_client.Connected)
                {
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
                            stream.ReadExactly(headPart);
                            packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart));
                            //bypass = false;
                            break;
                        default:
                            //bypass = true;
                            Console.WriteLine("Pad: " + BitConverter.ToString(pad));
                            Console.WriteLine("Common: " + BitConverter.ToString(commonPart)); 
                            throw new Exception($"unsupported method: {method}");
                            break;
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

                    }
                    var accountId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 0));
                    var projectId = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(packet, 4));
                    var lastPos = 8;
                    long minTs = long.MinValue;
                    long maxTs = long.MaxValue;
                    lastPos += Converter.UnmarshalVarInt64(packet, ref minTs, lastPos);
                    lastPos += Converter.UnmarshalVarInt64(packet, ref maxTs, lastPos);
                    if (minTs < Program.endDate && Program.startDate < maxTs)
                    {
                        //Console.WriteLine("Going to vmstorage");
                        if (vmstorStream == null)
                        {
                            vmstorStream = Program.connectionPool.GetClient();
                        }
                        
                        var buffer = ArrayPool<byte>.Shared.Rent(1 * 1024 * 1024);

                        zstdMBSent = true;
                        try
                        {
                            var cts = new CancellationTokenSource();
                            vmstorStream.Write(pad);
                            vmstorStream.Write(Converter.MarshalString(method));
                            vmstorStream.Write(commonPart);
                            if (prefix.Length > 0)
                            {
                                vmstorStream.Write(prefix);
                            }
                            vmstorStream.Write(headPart);
                            vmstorStream.Write(packet);
                            if (postfix.Length > 0)
                            {
                                vmstorStream.Write(postfix);
                            }
                            vmstorStream.Flush();
                            //Console.WriteLine("Sent request to vmstorage");

                            //Task.Delay(1000);
                            int bytesRead = 0;

                            int totalRead = 0;
                            var completion = Task.Run(() =>
                            {
                                int maxDecompressed = 0;
                                int decompRead = 0;
                                bool isCompleted = false;
                                bool startMarkerRead = false;
                                var decompressed = ArrayPool<byte>.Shared.Rent(1 * 1024 * 1024); // new byte[1024 * 1024];

                                int currPos = 0;
                                int blockCount = 0;
                                ulong blockSize = 0;
                                try
                                {
                                    while (!isCompleted)
                                    {
                                        //Console.WriteLine("WaitForDecomp");
                                        if (currPos < 0)
                                        {
                                            currPos = Math.Abs(currPos);
                                            Array.Copy(decompressed, decompRead - currPos, decompressed, 0, currPos);
                                            decompRead = decomp.Read(decompressed, currPos, decompressed.Length - currPos);
                                            decompRead += currPos;
                                            currPos = 0;
                                        }
                                        else
                                        {
                                            decompRead = decomp.Read(decompressed);
                                        }


                                        if (decompRead > maxDecompressed)
                                            maxDecompressed = decompRead;
                                        //Console.WriteLine($"Decompressed {decompRead}");
                                        if (!startMarkerRead)
                                        {
                                            var empty = Converter.UnmarshalString(decompressed);
                                            if (empty != "")
                                            {
                                                throw new Exception("kaka");
                                            }
                                            else
                                            {
                                                currPos = 8;
                                                startMarkerRead = true;
                                            }
                                            blockSize = Converter.UnmarshalUint64(decompressed, 8);
                                            blockCount = 1;
                                            currPos += 8 + (int)blockSize;
                                        }

                                        while (currPos < decompRead - 8 && blockSize != 0)
                                        {
                                            blockSize = Converter.UnmarshalUint64(decompressed, currPos);
                                            currPos = currPos + 8 + (int)blockSize;
                                            blockCount++;
                                        }
                                        if (blockSize == 0)
                                        {
                                            //Console.WriteLine("Empty block");
                                        }
                                        if (decompRead - currPos == 8)
                                        {
                                            try
                                            {
                                                var complete = Converter.UnmarshalLongString(decompressed, decompRead - 8);
                                                if (complete == "")
                                                {
                                                    isCompleted = true;
                                                }
                                            }
                                            catch (Exception) { }
                                        }
                                        else
                                        {
                                            currPos = currPos - decompRead;
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("266-" + ex.Message + "\r\n" + ex.StackTrace);
                                }
                                finally
                                {
                                    ArrayPool<byte>.Shared.Return(decompressed);
                                }

                                //Console.WriteLine($"Complete. Max Read {maxDecompressed}");
                                cts.Cancel();
                            });
                            try
                            {
                                //Console.WriteLine("Reading response from vmstorage");
                                while ((bytesRead = await vmstorStream.ReadAsync(buffer, cts.Token)) > 0)
                                {
                                    totalRead += bytesRead;
                                    //Console.WriteLine($"Got {bytesRead} bytes, total {totalRead}");//, clientPipe pos {pipeClient.}, serverPipe pos {pipeServer.Position}");
                                    await stream.WriteAsync(buffer, 0, bytesRead);
                                    await pipe.Writer.AsStream().WriteAsync(buffer, 0, bytesRead);
                                    //Console.WriteLine($"read: {bytesRead} bytes: {BitConverter.ToString(buffer.Take(bytesRead).ToArray())}");
                                    if (!cts.IsCancellationRequested)
                                    {
                                        
                                    }

                                }
                            }

                            catch (OperationCanceledException)
                            {
                                //Console.WriteLine("Cancelled");
                            }
                            catch (SocketException)
                            {

                            }
                            await completion;
                            //Console.WriteLine($"Last read: {bytesRead} Last bytes: {BitConverter.ToString(buffer.Take(bytesRead).ToArray())}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("302-" + ex.ToString() + "\r\n" + ex.StackTrace);
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(buffer);
                            //Program.connectionPool.ReturnClient(vmstorStream);
                        }
                    }
                    else
                    {
                        
                        if (!zstdMBSent)
                        {
                            await stream.WriteAsync(zstdMagicBytes);
                            //pipe.Writer.AsStream().Write(zstdMagicBytes);
                            zstdMBSent = true;
                        }
                        await stream.WriteAsync(emptyResponse);
                        //Console.WriteLine($"Period {minTs} to {maxTs} not inside selected month. Sending empty response");
                        //_client.Close();
                    }

                    //stream.Flush();
                    //vmstorStream.Flush();
                    //_client.Close();

                }
                if (vmstorStream != null)
                {
                    Program.connectionPool.DestroyClient(vmstorStream);
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
                    Console.WriteLine("connection closed");
                    (_client as IDisposable).Dispose();
                    _client = null;
                }
            }
        }

        
    }
}
