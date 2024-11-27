using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace VictoriaCheckProxy
{
    internal class Converter
    {
        
        public static string UnmarshalString(Stream reader)
        {
            UInt16 length = Converter.UnmarshalUint16(reader);
            var bytes = ArrayPool<byte>.Shared.Rent(128);
            reader.ReadExactly(bytes, 0, length);
            var result = Encoding.UTF8.GetString(bytes, 0, length);
            ArrayPool<byte>.Shared.Return(bytes);
            return result;
        }


        public static string UnmarshalLongString(byte[] buffer, int start = 0)
        {
            UInt64 length = Converter.UnmarshalUint64(buffer, start);
            return Encoding.UTF8.GetString(buffer, start + 8, (int)length);
        }

        

        public static byte[] ReadLongString(Stream reader)
        {
            UInt64 length = Converter.UnmarshalUint64(reader);
            byte[] buf = ArrayPool<byte>.Shared.Rent((int)(length + 8)); // new byte[length + 8];

            MarshalUint64(length).CopyTo(buf, 0);

            reader.ReadExactly(buf, 8, (int)length);
            return buf;
        }

        public static async Task<byte[]> ReadLongStringAsync(Stream reader)
        {
            UInt64 length = await Converter.UnmarshalUint64Async(reader);
            byte[] buf = new byte[length + 8];

            MarshalUint64(length).CopyTo(buf, 0);

            await reader.ReadAsync(buf, 8, (int)length);
            return buf;
        }

        public static byte[] MarshalString(string str)
        {
            var bytes = Encoding.UTF8.GetBytes("\0\0"+str);
            var lenBytes = MarshalUint16((ushort)(bytes.Length-2));
            bytes[0] = lenBytes[0];
            bytes[1] = lenBytes[1];
            return bytes;
        }


        public static byte[] MarshalUint64(UInt64 num)
        {
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(num));
            //return reader.ReadUInt16();
        }


        public static ushort UnmarshalUint16(Stream reader)
        {
            /*if (BitConverter.IsLittleEndian)
                span.Reverse();*/
            var bytes = ArrayPool<byte>.Shared.Rent(2);
            reader.ReadExactly(bytes, 0, 2);
            var tmp = bytes[0];
            bytes[0] = bytes[1];
            bytes[1] = tmp;
            var result = BitConverter.ToUInt16(bytes, 0);
            ArrayPool<byte>.Shared.Return(bytes);
            return result;
            //return reader.ReadUInt16();
        }

        public static uint UnmarshalUint32(Stream reader)
        {
            var bytes = ArrayPool<byte>.Shared.Rent(4);
            reader.ReadExactly(bytes, 0, 4);
            var result = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt32(bytes, 0));
            ArrayPool<byte>.Shared.Return(bytes);
            return result;
        }

        public static ulong UnmarshalUint64(Stream reader)
        {
            /*if (BitConverter.IsLittleEndian)
                span.Reverse();*/
            var bytes = ArrayPool<byte>.Shared.Rent(8);
            reader.ReadExactly(bytes, 0, 8);

            var result = BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt64(bytes, 0));
            ArrayPool<byte>.Shared.Return(bytes);
            return result;
        }

        public static async Task<ulong> UnmarshalUint64Async(Stream reader)
        {
            /*if (BitConverter.IsLittleEndian)
                span.Reverse();*/
            var bytes = new byte[8]; 
            await reader.ReadExactlyAsync(bytes);

            return BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt64(bytes, 0));
            //return reader.ReadUInt16();
        }

        public static string UnmarshalString(byte[] buff, int start = 0)
        {
            UInt16 length = Converter.UnmarshalUint16(buff, start);
            return Encoding.UTF8.GetString(buff, start + 2, length);
        }

        public static ushort UnmarshalUint16(byte[] buff, int start = 0)
        {
            return BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt16(buff, start));
            //return reader.ReadUInt16();
        }

        public static ulong UnmarshalUint64(byte[] buff, int start = 0)
        {
            return BinaryPrimitives.ReverseEndianness(BitConverter.ToUInt64(buff, start));
            //return reader.ReadUInt16();
        }

        public static byte[] MarshalUint16(UInt16 num)
        {
            return BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness(num));
            //return reader.ReadUInt16();
        }

        public static uint SwapBytes(uint x)
        {
            // swap adjacent 16-bit blocks
            x = (x >> 16) | (x << 16);
            // swap adjacent 8-bit blocks
            return ((x & 0xFF00FF00) >> 8) | ((x & 0x00FF00FF) << 8);
        }

        public static int UnmarshalVarInt64(byte[] span, ref Int64 value, int start = 0)
        {
            int i = 0;
            value = span[start + i++];
            if (value < 128)
            {
                value = (value >> 1) ^ ((value << 7) >> 7);
                return i;
            }
            value &= 0x7f;
            int shift = 7;
            do
            {
                byte b = span[start + i++];
                value |= (Int64)(b & 0x7F) << shift;
                if (b < 0x80)
                {
                    break;
                }
                shift += 7;
            }
            while (shift < 64);
            value = (value >> 1) ^ ((value << 63) >> 63);
            return i;
        }
    }
}
