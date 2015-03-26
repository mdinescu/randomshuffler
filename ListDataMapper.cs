using System;
using System.Collections.Generic;
using System.Data;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;

namespace DNQ.RandomSuffler
{

    /// <summary>
    /// A data reader that implementation for parse lists of 32bit integers,
    /// previously saved by the shuffle operation (via memory mapped files).
    /// 
    /// This class functions as a specialized adapter that reads the numbers
    /// from the file and presents them in the form that can be consumed via
    /// the IDataReader interface.
    /// </summary>
    public class ListDataMapper
            : IDataReader
    {
        private System.IO.FileInfo fileInfo;
        private MemoryMappedFile memMap;
        private MemoryMappedViewAccessor memView;
        private long _offset = -1;
        private long _index = 0;

        /// <summary>
        /// Instantiates a new ListDataMapper from a given file name.
        /// Users should call Dispose when finished with the mapper.
        /// </summary>
        /// <param name="file">A file that contains the shuffled numbers in binary form.</param>
        public ListDataMapper(string file)
        {
            fileInfo = new System.IO.FileInfo(file);

            memMap = MemoryMappedFile.CreateFromFile(file, System.IO.FileMode.Open, "ListMap", fileInfo.Length, MemoryMappedFileAccess.ReadWrite);
            memView = memMap.CreateViewAccessor();
            _offset = 0;
        }

        public void Close()
        {
            Dispose();
        }

        public int Depth
        {
            get { return 0; }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get;
            private set;
        }

        public bool NextResult()
        {
            return false;
        }

        /// <summary>
        /// Call this method to advance the reader to the next record (number in the suffled list).
        /// </summary>
        /// <returns></returns>
        public bool Read()
        {
            if (IsClosed)
                throw new ObjectDisposedException("Cannot read from a disposed mapper");
            if (_offset == -1)
                _offset = 0;
            else
            {
                _offset += 4; _index++;
            }
            return (_offset < fileInfo.Length);
        }

        public int RecordsAffected
        {
            get { return 1; }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (memView != null)
                {
                    memView.Dispose();
                    memView = null;
                }
                if (memMap != null)
                {
                    memMap.Dispose();
                    memMap = null;
                }
            }
            IsClosed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Number of fields presented by the IDataReader interface.
        /// </summary>
        public int FieldCount
        {
            get { return 4; }
        }

        public bool GetBoolean(int i)
        {
            if (i == 2)
                return false;
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public byte GetByte(int i)
        {
            if (i == 2)
                return (byte)(((memView.ReadUInt32(_offset) & (uint)0x80000000) != 0) ? 1 : 0);
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public char GetChar(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public IDataReader GetData(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public string GetDataTypeName(int i)
        {
            switch (i)
            {
                case 0:
                    return "Int64";
                case 1:
                    return "Int32";
                case 2:
                    return "Byte";
                case 3:
                    return "Int32";
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public DateTime GetDateTime(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public decimal GetDecimal(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public double GetDouble(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public Type GetFieldType(int i)
        {
            switch (i)
            {
                case 0:
                    return typeof(Int64);
                case 1:
                    return typeof(Int32);
                case 2:
                    return typeof(Byte);
                case 3:
                    return typeof(Int32);
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public float GetFloat(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public Guid GetGuid(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public short GetInt16(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        /// <summary>
        /// Returns the value as a 32-bit integer, for a given field. In this implementation
        /// the only field that can be read as a 32-bit integer is field 1, the shuffled number.
        /// </summary>
        /// <param name="i">The field index.</param>
        /// <returns>A 32-bit integer - the value of the current number in the list of shuffled numbers.</returns>
        public int GetInt32(int i)
        {
            if (i == 1)
                return memView.ReadInt32(_offset);
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        /// <summary>
        /// Returns the value as a 64-bit integer, for a given field. In this implementation
        /// the only field that can be read as a 64-bit integer is field 0, the index position.
        /// </summary>
        /// <param name="i">The field index.</param>
        /// <returns>A 64-bit integer - the index of the current number in the list of shuffled numbers.</returns>
        public long GetInt64(int i)
        {
            if (i == 0)
                return _index;
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public string GetName(int i)
        {
            switch (i)
            {
                case 0:
                    return "CodeIndex";
                case 1:
                    return "Code";
                case 2:
                    return "CodeType";
                case 3:
                    return "Assigned";
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public int GetOrdinal(string name)
        {
            switch (name)
            {
                case "CodeIndex":
                    return 0;
                case "Code":
                    return 1;
                case "CodeType":
                    return 2;
                case "Assigned":
                    return 3;
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public string GetString(int i)
        {
            throw new InvalidCastException("Invalid data type for column " + i.ToString());
        }

        public object GetValue(int i)
        {
            switch (i)
            {
                case 0:
                    return GetInt64(0);
                case 1:
                    return GetInt32(1);
                case 2:
                    return GetByte(2);
                case 3:
                    return DBNull.Value;
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            if (i == 3) return true;
            return false;
        }

        public object this[string name]
        {
            get
            {
                switch (name)
                {
                    case "CodeIndex":
                        return GetInt64(0);
                    case "Code":
                        return GetInt32(1);
                    case "CodeType":
                        return GetByte(2);
                    case "Assigned":
                        return DBNull.Value;
                    default:
                        throw new IndexOutOfRangeException();
                }
            }
        }

        public object this[int i]
        {
            get
            {
                switch (i)
                {
                    case 0:
                        return GetInt64(0);
                    case 1:
                        return GetInt32(1);
                    case 2:
                        return GetByte(2);
                    case 3:
                        return DBNull.Value;
                    default:
                        throw new IndexOutOfRangeException();
                }
            }
        }
    }

}
