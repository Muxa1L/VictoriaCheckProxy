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
        public static async Task Main(string[] args)
        {
            if (args.Length == 0)
                throw new Exception("2 args expected");
            var storedMonth = args[0];
            storageEP = args[1];
            DateTimeOffset date = DateTime.ParseExact(storedMonth, "yyyy-MM", CultureInfo.InvariantCulture);
            startDate = date.ToUnixTimeMilliseconds();
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

    class ClientWorking
    {
        //const string vminsertHello = "vminsert.02";
        const string vmselectHello = "vmselect.01";
        const string successResponse = "ok";
        //const string searchMethod = "search_v7";
        const bool isCompressed = true;

        private static readonly byte[] emptyResponse = new byte[] { 0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x60, 0x44, 0x00, 0x00, 0x08, 0x00, 0x01, 0x54, 0x01, 0x02, 0x14, 0x04 };

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
                //Console.WriteLine($"connection opened from {_client.Client.RemoteEndPoint.ToString()}");
                using (var stream = _client.GetStream())
                {
                    using (var sr = new BinaryReader(stream))
                    using (var sw = new BinaryWriter(stream))
                    {
                        ///Handshake begin
                        GetMessage(vmselectHello, sr);
                        SendMessage(successResponse, sw);
                        var isRemoteCompressed = sr.ReadBoolean();
                        SendMessage(successResponse, sw);
                        sw.Write(isCompressed);
                        GetMessage(successResponse, sr);
                        ///Handshake end 

                        //while (true)
                        {
                            /*var head = sr.ReadBytes(6 + 11 + 1 + 4 + 8);
                            string method = Converter.UnmarshalString(head, 6);
                            long packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(head, 22));*/


                            //Padding из 6 нулевых байтов
                            var pad = sr.ReadBytes(6);

                            string method = Converter.UnmarshalString(sr);
                            //bool bypass = false;
                            byte[] commonPart = sr.ReadBytes(5); //tracing flag + timeout
                            byte[] prefix = new byte[0];
                            byte[] headPart = new byte[] { 0x00 }; ;
                            byte[] postfix = new byte[0];

                            long packetSize = 0;
                            switch (method)
                            {
                                case "labelValues_v5":
                                    prefix = Converter.ReadLongString(sr);
                                    headPart = sr.ReadBytes(8);
                                    packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart));

                                    //tagName = Converter.UnmarshalString(sr);
                                    //shift += tagName.Length + 2;
                                    break;
                                case "labelNames_v5":
                                case "search_v7":
                                    headPart = sr.ReadBytes(8);
                                    packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart));
                                    //bypass = false;
                                    break;
                                default:
                                    //bypass = true;
                                    throw new Exception($"unsupported method: {method}");
                                    break;
                            }

                            //bool traceEnabled = sr.ReadBoolean();
                            //uint timeout = BinaryPrimitives.ReverseEndianness(sr.ReadUInt32());
                            //long packetSize = BinaryPrimitives.ReverseEndianness(sr.ReadInt64());
                            //long packetSize = BinaryPrimitives.ReverseEndianness(BitConverter.ToInt64(headPart, 5)); 
                            var packet = sr.ReadBytes((int)packetSize);
                            switch (method)
                            {
                                case "labelNames_v5":
                                case "labelValues_v5":
                                    postfix = sr.ReadBytes(4);
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
                                Console.WriteLine("Going to vmstorage");
                                try
                                {
                                    using (TcpClient tcpClient = new TcpClient())
                                    {
                                        tcpClient.ReceiveTimeout = 30 * 1000;
                                        tcpClient.Connect(IPEndPoint.Parse(Program.storageEP));
                                        var buffer = ArrayPool<byte>.Shared.Rent(1 * 1024 * 1024); //new byte[64 * 1024 * 1024];
                                        var pipeBuffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);
                                        //new System.IO.Pipes.AnonymousPipeServerStream
                                        try
                                        {
                                            //pipeClient.SafePipeHandle
                                            //pipeServer.InBufferSize = 2 * 1024 * 1024;
                                            var pipe = new Pipe();
                                            //using (var pipeServer = new AnonymousPipeServerStream()) 
                                            //using (var pipeServer = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.Inheritable, 20*1024*1024))
                                            using (var vmstorStream = tcpClient.GetStream())
                                            using (var vmsr = new BinaryReader(vmstorStream))
                                            using (var vmsw = new BinaryWriter(vmstorStream))
                                            //, checkEndOfStream: false, leaveOpen: false))
                                            {
                                                var cts = new CancellationTokenSource();
                                                //decomp.SetParameter(ZstdSharp.Unsafe.ZSTD_dParameter.ZSTD_d_windowLogMax, 31);
                                                SendMessage(vmselectHello, vmsw);
                                                GetMessage(successResponse, vmsr);
                                                vmsw.Write(isRemoteCompressed);
                                                GetMessage(successResponse, vmsr);
                                                var comp = vmsr.ReadBoolean();
                                                SendMessage(successResponse, vmsw);

                                                vmsw.Write(pad);
                                                vmsw.Write(Converter.MarshalString(method));
                                                vmsw.Write(commonPart);
                                                if (prefix.Length > 0)
                                                {
                                                    vmsw.Write(prefix);
                                                }
                                                vmsw.Write(headPart);
                                                vmsw.Write(packet);
                                                if (postfix.Length > 0)
                                                {
                                                    vmsw.Write(postfix);
                                                }
                                                vmstorStream.Flush();
                                                

                                                //await Task.Delay(1000);
                                                int bytesRead = 0;

                                                int totalRead = 0;
                                                var completion = Task.Run(async () =>
                                                {
                                                    int maxDecompressed = 0;

                                                    //using (var pipeClient = new AnonymousPipeClientStream(pipeServer.GetClientHandleAsString())) // = new AnonymousPipeClientStream(PipeDirection.In, pipeServer.GetClientHandleAsString()))
                                                    //using (var decomp = new DecompressionStream(pipeClient, bufferSize: 100 * 1024 * 1024))
                                                    using (var decomp = new DecompressionStream(pipe.Reader.AsStream()))
                                                    {
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
                                                                    decompRead = await decomp.ReadAsync(decompressed, currPos, decompressed.Length - currPos);
                                                                    decompRead += currPos;
                                                                    currPos = 0;
                                                                }
                                                                else
                                                                {
                                                                    decompRead = await decomp.ReadAsync(decompressed);
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
                                                                    //Console.WriteLine($"Block num {blockCount} size {blockSize} new pos {currPos} total decomp {decompRead}");
                                                                }
                                                                if (blockSize == 0)
                                                                {
                                                                    Console.WriteLine();
                                                                }
                                                                if (decompRead - currPos == 8)
                                                                {
                                                                    try
                                                                    {
                                                                        var complete = Converter.UnmarshalLongString(decompressed, decompRead - 8);
                                                                        //complete2 = Converter.UnmarshalLongString(decompressed, decompRead - 16);
                                                                        if (complete == "")// && complete2 == "")
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


                                                                //string complete2 = "test";



                                                            }
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            Console.WriteLine(ex.Message);
                                                        }
                                                        finally
                                                        {
                                                            ArrayPool<byte>.Shared.Return(decompressed);
                                                        }
                                                    }

                                                    Console.WriteLine($"Complete. Max Read {maxDecompressed}");
                                                    cts.Cancel();
                                                });
                                                try
                                                {
                                                    while ((bytesRead = await vmstorStream.ReadAsync(buffer, cts.Token)) > 0)
                                                    {
                                                        totalRead += bytesRead;
                                                        //Console.WriteLine($"Got {bytesRead} bytes, total {totalRead}");//, clientPipe pos {pipeClient.}, serverPipe pos {pipeServer.Position}");
                                                        await stream.WriteAsync(buffer, 0, bytesRead);
                                                        
                                                        if (!cts.IsCancellationRequested)
                                                        {
                                                            await pipe.Writer.AsStream().WriteAsync(buffer, 0, bytesRead);
                                                        }
                                                        //Console.WriteLine("loop");
                                                        //await pipeServer.FlushAsync();
                                                        //await pipeServer.FlushAsync();
                                                        /*if (totalRead > 12)
                                                        {
                                                            //decomp.
                                                            //decomp.end
                                                            /*decomp.
                                                            var test = decomp.ReadByte();
                                                            decompRead = decomp.Read(buffer);
                                                            //decomp.Rea

                                                            var complete = Converter.UnmarshalLongString(decompressed, decompRead - 8);
                                                            //var error = Converter.UnmarshalLongString(decompressed, decompRead - 16);
                                                            if (complete == "")
                                                                break;
                                                        }*/
                                                        //decompRead = decomp.Read(buffer);

                                                        /*if ((decompRead = await decomp.ReadAsync(decompressed)) > 0)
                                                        {
                                                            Console.WriteLine("");
                                                        }*/



                                                    }
                                                    //await stream.FlushAsync();
                                                }
                                                catch (OperationCanceledException) { }
                                                /*bytesRead = await vmstorStream.ReadAsync(buffer);
                                                if (bytesRead == 0)
                                                {
                                                    Console.WriteLine();
                                                }
                                                Console.WriteLine("");*/
                                                /* while (true)
                                             {
                                                 read = vmsr.Read(buf);
                                                 sw.Write(buf, 0, read);
                                                 pipeServer.Write(buf, 0, read);

                                                 //decSize = dsr.Read(decompressed);//dsr.Read(decompressed);
                                                 //decomp.
                                                 decSize = decomp.Read(decompressed);
                                                 decomp.
                                                 Console.WriteLine($"{read} {decSize} {BitConverter.ToString(buf, 0, read)}");
                                                 var one = Converter.UnmarshalUint64(decompressed, decSize - 8);
                                                 var two = Converter.UnmarshalUint64(decompressed, decSize - 16);
                                                 var three = Converter.UnmarshalUint64(decompressed, decSize - 24);
                                                 //var four = Converter.UnmarshalUint64(decompressed, decSize - 32);
                                                 if ((decSize == 0 && read != 0) || read == 0)
                                                 {
                                                     sw.Flush();
                                                     break;
                                                 }
                                             }*/
                                                /*var toVmselect = vmstorStream.CopyToAsync(stream);
                                                var toVmstorage = stream.CopyToAsync(vmstorStream);
                                                Task.WaitAny(new Task[] { toVmselect, toVmstorage });
                                                Console.WriteLine("exchange completed");*/
                                            }
                                        }
                                        finally
                                        {
                                            ArrayPool<byte>.Shared.Return(buffer);
                                            
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex.ToString());
                                }
                                finally
                                {

                                }
                            }
                            else
                            {
                                Console.WriteLine($"Period {minTs} to {maxTs} not inside selected month. Sending empty response");
                                sw.Write(emptyResponse);
                            }
                            //tStartA < tEndB && tStartB < tEndA;
                            //if ((maxTs > Program.startDate && maxTs < Program.endDate) || (minTs > Program.startDate && minTs < Program.endDate))

                            sw.Flush();
                            _client.Close();
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
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

        public static void SendMessage(string msg, BinaryWriter sw)
        {
            sw.Write(msg.ToCharArray());
        }

        public static void GetMessage(string msg, BinaryReader sr)
        {
            byte[] buffer = new byte[msg.Length];
            var rqSize = sr.Read(buffer, 0, msg.Length);
            var recv = Encoding.UTF8.GetString(buffer);
            if (recv != msg)
            {
                throw new Exception($"Говна какая-то {recv} вместо {msg}");
            }
        }
    }
}
