using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VictoriaCheckProxy
{
    public static class Helpers
    {
        public static void SendMessage(string msg, Stream sw, bool isDbg = false)
        {
            sw.Write(Encoding.ASCII.GetBytes(msg));
            if (isDbg)
            {
                Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} Sent {msg}");
            }
        }

        public static void GetMessage(string msg, Stream sr, bool isDbg = false)
        {
            byte[] buffer = new byte[msg.Length];
            var rqSize = sr.Read(buffer, 0, msg.Length);
            var recv = Encoding.UTF8.GetString(buffer);
            if (isDbg)
            {
                Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} Got {recv} {rqSize} while awaiting {msg}");
            }
            if (recv != msg)
            {
                throw new Exception($"{Thread.CurrentThread.ManagedThreadId} Говна какая-то {BitConverter.ToString(buffer)} вместо {msg}");
            }
        }
    }
}
