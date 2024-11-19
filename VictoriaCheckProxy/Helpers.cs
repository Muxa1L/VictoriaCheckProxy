using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VictoriaCheckProxy
{
    public static class Helpers
    {
        public static void SendMessage(string msg, Stream sw)
        {
            sw.Write(Encoding.ASCII.GetBytes(msg));
        }

        public static void GetMessage(string msg, Stream sr)
        {
            byte[] buffer = new byte[msg.Length];
            var rqSize = sr.Read(buffer, 0, msg.Length);
            var recv = Encoding.UTF8.GetString(buffer);
            if (recv != msg)
            {
                throw new Exception($"Говна какая-то {BitConverter.ToString(buffer)} вместо {msg}");
            }
        }
    }
}
