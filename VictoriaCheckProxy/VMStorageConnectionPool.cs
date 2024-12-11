using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;
using System.Buffers;

namespace VictoriaCheckProxy
{
    internal class VMStorageConnection: IDisposable
    {
        internal NetworkStream networkStream;
        internal DecompressionStream decompressor;
        internal TcpClient tcpClient;
        public VMStorageConnection()
        {
            Console.WriteLine("Creating new client");
            tcpClient = new TcpClient();
            tcpClient.ReceiveTimeout = 60 * 1000;
            tcpClient.SendTimeout = 60 * 1000;

            tcpClient.Connect(IPEndPoint.Parse(Program.storageEP));
            //new byte[64 * 1024 * 1024];
            //var pipeBuffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);

            networkStream = tcpClient.GetStream();
            networkStream.Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            var test = networkStream.Socket.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval);
            networkStream.Socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 60);
            test = networkStream.Socket.GetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval);
            //, checkEndOfStream: false, leaveOpen: false))

            //decomp.SetParameter(ZstdSharp.Unsafe.ZSTD_dParameter.ZSTD_d_windowLogMax, 31);
            Helpers.SendMessage(ClientWorking.vmselectHello, networkStream);
            Helpers.GetMessage(ClientWorking.successResponse, networkStream);
            networkStream.WriteByte(0);
            Helpers.GetMessage(ClientWorking.successResponse, networkStream);
            var comp = networkStream.ReadByte();
            Helpers.SendMessage(ClientWorking.successResponse, networkStream);
            decompressor = new DecompressionStream(networkStream, 10 * 1024 * 1024);
        }

        public void Dispose()
        {
            try
            {
                decompressor.Dispose();
            }
            catch (Exception) { }
            try
            {
                networkStream.Dispose();
            }
            catch (Exception) { }
            try
            {
                tcpClient.Dispose();
            }
            catch (Exception) { }
            decompressor = null;
            networkStream = null;
            tcpClient = null;
        }
    }
    internal class VMStorageConnectionPool
    {
        ConcurrentQueue<VMStorageConnection> connections;
        //ConcurrentBag<TcpClient> freeConnections;
        readonly int size = 0;
        internal readonly SemaphoreSlim limit;
        public VMStorageConnectionPool(int size)
        {
            connections = new ConcurrentQueue<VMStorageConnection>();
            this.size = size;
            limit = new SemaphoreSlim(size, size);
        }


        public VMStorageConnection GetClient()
        {
            limit.Wait();
            VMStorageConnection result;
            if (connections.TryDequeue(out result)) {
                bool check = false;
                try
                {
                    check = !(result.networkStream.Socket.Poll(1, SelectMode.SelectRead) && result.networkStream.Socket.Available == 0);
                }
                catch (SocketException) { check = false; }
                if (!check)
                {
                    Console.WriteLine("Client is closed - recreating");
                    result.Dispose();
                    result = null;
                }
            }
            if (result == null) 
            {
                result = new VMStorageConnection();
            }
            else if (result.networkStream.DataAvailable)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);
                try
                {
                    Console.WriteLine("Unread data in vmstorage connection. Reading");
                    var got = result.decompressor.Read(buffer);
                    Console.WriteLine($"Got {got} bytes");
                    Console.WriteLine(BitConverter.ToString(buffer));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
                
            }
            return result;
        }
        public void ReturnClient(VMStorageConnection client)
        {
            connections.Enqueue(client);
            limit.Release();
        }

        public void DestroyClient(VMStorageConnection client)
        {
            limit.Release();
            client.networkStream.Close();
            client.Dispose();
        }
    }
}
