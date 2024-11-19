using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace VictoriaCheckProxy
{
    internal class VMStorageConnectionPool
    {
        ConcurrentQueue<NetworkStream> connections;
        //ConcurrentBag<TcpClient> freeConnections;
        readonly int size = 0;
        readonly SemaphoreSlim limit;
        public VMStorageConnectionPool(int size)
        {
            connections = new ConcurrentQueue<NetworkStream>();
            this.size = size;
            limit = new SemaphoreSlim(size, size);
        }


        public NetworkStream GetClient()
        {
            limit.Wait();
            NetworkStream result;
            if (connections.TryDequeue(out result)) {
                bool check = false;
                try
                {
                    check = !(result.Socket.Poll(1, SelectMode.SelectRead) && result.Socket.Available == 0);
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
                Console.WriteLine("Creating new client");
                var tcpClient = new TcpClient();
                tcpClient.ReceiveTimeout = 30 * 1000;
                tcpClient.Connect(IPEndPoint.Parse(Program.storageEP));
                //new byte[64 * 1024 * 1024];
                //var pipeBuffer = ArrayPool<byte>.Shared.Rent(10 * 1024 * 1024);

                result = tcpClient.GetStream();
                //, checkEndOfStream: false, leaveOpen: false))

                //decomp.SetParameter(ZstdSharp.Unsafe.ZSTD_dParameter.ZSTD_d_windowLogMax, 31);
                Helpers.SendMessage(ClientWorking.vmselectHello, result);
                Helpers.GetMessage(ClientWorking.successResponse, result);
                result.WriteByte(0);
                Helpers.GetMessage(ClientWorking.successResponse, result);
                var comp = result.ReadByte();
                Helpers.SendMessage(ClientWorking.successResponse, result);
            }
            return result;
        }
        public void ReturnClient(NetworkStream client)
        {
            connections.Enqueue(client);
            limit.Release();
        }

        public void DestroyClient(NetworkStream client)
        {
            limit.Release();
            client.Close();
            client.Dispose();
        }
    }
}
