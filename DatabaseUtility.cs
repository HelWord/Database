using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
namespace Yangsi
{
    /// <summary>DatabaseUtility
    /// <para>是杨思信息科技开发的一款通用数据库操作库，</para>
    /// <para>将底层数据库驱动进行二次封装，提供简单的接口完成操作。</para>
    /// <para>提供自定义操作接口，可执行用户输入的语句。</para>
    /// <para>注意，目前仅支持二重条件筛选。</para>
    /// <para>数据库支持int、string、float、DateTime数据类型。</para>
    /// </summary>
    public class DatabaseUtility:IDisposable
    {
        #region 字段
        private uint maxColumnNumber;
        private string connectionString;
        private string databaseName;
        private ulong maxTableSize;
        private SqlConnection sqlCon;
        private bool disposed = false;
        private static Dictionary<Type, String> supportedTypeDict;
        #endregion

        #region 构造函数
        /// <summary>
        /// 静态构造函数
        /// <para>用来初始化支持的数据类型字典</para>
        /// </summary>
        static DatabaseUtility()
        {
            supportedTypeDict = new Dictionary<Type, String> { 
                {typeof(int),"INTEGER "},
                {typeof(string),"NTEXT"},
                {typeof(float),"FLOAT(24)"},
                {typeof(DateTime),"DATETIME"},
                };
        }

        /// <summary>
        /// 默认构造函数
        /// </summary>
        public DatabaseUtility()
        {
            connectionString = "";
            databaseName = "";
            maxColumnNumber = 0;
            maxTableSize = 0;
            sqlCon = new SqlConnection();
        }
        /// <summary>
        /// 构造函数
        /// <para>connect参数必须是包含完整数据库连接信息的参数</para>
        /// <para>如无法提供完整参数，请使用无参构造函数后，调用BuildConnectionString接口</para>
        /// </summary>
        /// <param name="connect">用于连接数据库的字符串</param>
        public DatabaseUtility(string connect)
        {
            this.ConnectionString = connect;
        }
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connect">用于连接数据库的字符串</param>
        /// <param name="tableSize">表的最大行数</param>
        public DatabaseUtility(string connect, ulong tableSize)
            : this(connect)
        {
            this.MaxTableRowSize = tableSize;
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connect">用于连接数据库的字符串</param>
        /// <param name="tableSize">表的最大行数</param>
        /// <param name="columnSize">表的最大列数</param>
        public DatabaseUtility(string connect, ulong tableSize, uint columnSize)
            : this(connect, tableSize)
        {
            this.MaxTableColumnSize = columnSize;
        }
        #endregion

        #region 析构函数
        /// <summary>
        /// Dispose函数
        /// <para>用于执行资源释放等功能</para>
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);      
        }

        /// <summary>
        /// 实际执行Dispose的函数
        /// 密封类修饰用private
        /// </summary>
        /// <param name="disposing">区别对待托管资源和非托管资源</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }
            if(disposing)
            { 
                //释放托管资源
            }
            //释放非托管资源
            if (sqlCon != null && sqlCon.State == ConnectionState.Open)
            {
                sqlCon.Close();
            }
            sqlCon.Dispose();
            sqlCon = null;
            disposed = true;
        }

        /// <summary>
        /// 必须，以备忘记了显式调用Dispose方法
        /// </summary>
        ~DatabaseUtility()
        {
            this.Dispose(false);
        }
        #endregion

        #region 枚举
        /// <summary>
        /// 数字筛选的方式
        /// </summary>
        public enum NumericSelectCriteria
        {
            /// <summary>
            /// 小于
            /// </summary>
            LessThan,
            /// <summary>
            /// 大于
            /// </summary>
            GreaterThan,
            /// <summary>
            /// 小于等于
            /// </summary>
            LessThanOrEqual,
            /// <summary>
            /// 大于等于
            /// </summary>
            GreaterThanOrEqual,
            /// <summary>
            /// 在区间内
            /// </summary>
            Between,
            /// <summary>
            /// 无
            /// </summary>
            None
        }

        /// <summary>
        /// 字符串筛选的方式
        /// </summary>
        public enum TxtSelectCriteria
        {
            /// <summary>
            /// 包含
            /// </summary>
            Contain,
            /// <summary>
            /// 不包含
            /// </summary>
            NoContain,
            /// <summary>
            /// 以开始
            /// </summary>
            StartWith,
            /// <summary>
            /// 以结尾
            /// </summary>
            EndWith,
            /// <summary>
            /// 无
            /// </summary>
            None
        }
        
        #endregion

        #region 属性
        /// <summary>
        /// 获得用于连接数据库的信息
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        public string ConnectionString
        {
            get
            {
                return connectionString;
            }
            private set
            {
                if (value != null && value.Length > 0)
                {
                    //不重复打开和关闭
                    if (value != connectionString)
                    {
                        connectionString = value;
                        if (sqlCon != null && sqlCon.State == ConnectionState.Open)
                        {
                            sqlCon.Close();
                        }
                        sqlCon = sqlCon ?? new SqlConnection();
                        sqlCon.ConnectionString = connectionString;
                        sqlCon.Open();
                    }
                }
                else
                {
                    throw new System.ArgumentException("Parameter is invalid.", "ConnectionString");
                }
            }
        }

        /// <summary>
        /// 获得或设置数据表的最大行数
        /// <para>0：无限制</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        public ulong MaxTableRowSize
        {
            get
            {
                return maxTableSize;
            }
            set
            {
                if (value < 0)
                {
                    throw new System.ArgumentException("Parameter can not be less than Zero.", "MaxTableRowSize");
                }
                maxTableSize = value;
            }
        }

        /// <summary>
        /// 获得或设置数据表的最大列数
        /// <para>0：无限制</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        public uint MaxTableColumnSize
        {
            get
            {
                return maxColumnNumber;
            }
            set
            {
                if (value < 0)
                {
                    throw new System.ArgumentException("Parameter can not be less than Zero.", "MaxTableColumnSize");
                }
                maxColumnNumber = value;
            }
        }
        #endregion

        #region 辅助方法

        /// <summary>
        /// <para>创建数据库连接字符串</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <param name="address">数据库服务器地址
        /// <para>.或localhost：本地</para>
        /// </param>
        /// <param name="databaseName">数据库名</param>
        /// <param name="id">用户名</param>
        /// <param name="pwd">密码</param>
        /// <returns></returns>
        public virtual void BuildConnectionString(string databaseName, string id, string pwd,string address=".")
        {
            if (address == null || address.Length < 1)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "Address");
            if (databaseName == null || databaseName.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty", "DatabaseName");
            if (id == null || pwd == null)
                throw new System.ArgumentException("Parameters cannot be null", "Id or Password");
            this.databaseName = databaseName;
            SqlConnectionStringBuilder sBuilder = new SqlConnectionStringBuilder();
            sBuilder.DataSource = address;
            sBuilder.InitialCatalog = databaseName;
            sBuilder.UserID = id;
            sBuilder.Password = pwd;
            sBuilder.IntegratedSecurity = true;
            this.ConnectionString = sBuilder.ConnectionString;
            return;
        }

        /// <summary>
        /// 判断数据库连接状态
        /// </summary>
        private void CheckDatabaseConnection()
        {
            //是否初始化
            sqlCon = sqlCon ?? new SqlConnection();
            //是否已打开
            if (sqlCon.State != ConnectionState.Open)
            {
                //连接信息是否已设置
                if (sqlCon.ConnectionString == null || sqlCon.ConnectionString.Length <= 0)
                {
                    //连接字符串是否已设置
                    if (ConnectionString == null || ConnectionString.Length <= 0)
                        throw new System.ArgumentNullException("ConnectionString", "Parameter is not initialized yet.");
                    sqlCon.ConnectionString = ConnectionString;
                }
                //打开
                sqlCon.Open();
            }
        }

        /// <summary>
        /// 获得数据库中所有的表
        /// </summary>
        public List<string> GetAllTables()
        {
            CheckDatabaseConnection();
            string queryString = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'  AND TABLE_CATALOG = @dbName;";
            //queryString = "SELECT SysObjects.name FROM sysobjects WHERE type = 'U' AND sysstat = '83'";
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@dbName", this.databaseName);
            SqlDataReader reader = command.ExecuteReader();
            List<string> str = new List<string>();
            try
            {
                while (reader.Read())
                {
                    str.Add(reader[0].ToString());
                }
            }
            finally
            {
                reader.Close();
            }
            return str;
        }
        /// <summary>
        /// 获得表中所有的列名
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <returns></returns>
        public virtual List<string> GetColumnsInTable(string table)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            string queryString = "SELECT COLUMN_NAME 'All_Columns' FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName ;";
            //queryString = "SELECT name FROM SysColumns WHERE id = Object_Id(@tableName)";
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@tableName", table);
            SqlDataReader reader = command.ExecuteReader();
            List<string> str = new List<string>();
            try
            {
                while (reader.Read())
                {
                    str.Add(reader[0].ToString());
                }
            }
            finally
            {
                reader.Close();
            }
            return str;
        }

        /// <summary>
        /// 获得表中当前的行数
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <returns></returns>
        public virtual ulong GetRowCountInTable(string table)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            string queryString = "SELECT COUNT(*) FROM @tableName ;";
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@tableName", table);
            return Convert.ToUInt64(command.ExecuteScalar());
        }

        /// <summary>
        /// 获得表中所有列及其数据类型
        /// </summary>
        /// <param name="table">表名</param>
        /// <returns></returns>
        public virtual Dictionary<string,Type> GetColumnsAndTypeInTable(string table)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            string queryString = "SELECT name AS column_name,TYPE_NAME(system_type_id) AS column_type, max_length,is_nullable FROM sys.columns WHERE object_id = OBJECT_ID(@tableName)";
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = queryString;
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@tableName", table);
            Dictionary<string, Type> dic = new Dictionary<string,Type>();
            SqlDataReader reader = command.ExecuteReader();
            try
            {
                while (reader.Read())
                {
                    dic.Add(reader[0].ToString(), reader[1].GetType());
                }
            }
            finally
            {
                reader.Close();
            }
            return dic;
        }

        /// <summary>
        /// 获得表中数据的时间区间
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="start">起始时间</param>
        /// <param name="end">结束时间</param>
        public virtual void GetTimeSpanInTable(string table, out DateTime start, out DateTime end)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            //第一步 查询表中是否存在DateTime列
            Dictionary<string, Type> t = this.GetColumnsAndTypeInTable(table);
            var dateCoumns =
                from entry in t
                where (entry.Value ==typeof(DateTime))
                select entry.Key;
            if (dateCoumns.Count() <= 0)
                throw new System.ArgumentException("There is no DateTime Column", "table");
            //表中可能有多个DateTime列，取第一个列
            string dateCoumnName = dateCoumns.ElementAt(0);
            //第二步 取最早和最晚的时间
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            string queryString = "select top 1 @columnName from @tableName order by @columnName";
            command.CommandText = queryString;
            command.Parameters.AddWithValue("@tableName", table);
            command.Parameters.AddWithValue("@columnName", dateCoumnName);
            start = Convert.ToDateTime(command.ExecuteScalar());

            queryString = "select top 1 @columnName from @tableName order by @columnName desc";
            command.CommandText = queryString;
            command.Parameters.AddWithValue("@tableName", table);
            command.Parameters.AddWithValue("@columnName", dateCoumnName);
            end = Convert.ToDateTime(command.ExecuteScalar());
            return;
        }

        /// <summary>
        /// 创建表
        /// <para>第一个参数表示列名，第二个参数表示该列数据类型，以此类推</para>
        /// <para>例如,
        /// <example>
        /// <code>
        /// <para>object[] list = new object[4];</para>
        /// <para>list[0] = "custom"; </para>
        /// <para>list[1] = DateTime.Now; </para>
        /// <para>list[2] = "cost"; </para>               
        /// <para>list[3] = 5.0;</para>
        /// <para>db.CreateTable("Smith", list);</para>
        /// </code>
        /// </example>
        /// </para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="list">参数列表</param>
        /// <returns></returns>
        public virtual bool CreateTable(string table, params object[] list)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter cannot be null or empty.", "table");
            if (list == null || list.Length <= 0 || list.Length % 2 != 0)
                throw new System.ArgumentException("insuffient table column names.", "list");
            CheckDatabaseConnection();
            if (this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table","name is already Exist.");
            StringBuilder sBuilder = new StringBuilder("CREATE TABLE ");
            sBuilder.Append(table);
            sBuilder.Append("(");
            for (int i = 0; i < list.Length; i += 2)
            {
                Type t = list[i + 1].GetType();
                if (supportedTypeDict.ContainsKey(t))
                {
                    sBuilder.Append(list[i].ToString());
                    sBuilder.Append(" ");
                    sBuilder.Append(supportedTypeDict[t]);
                    sBuilder.Append(",");
                }
                else
                {
                    throw new System.NotSupportedException(t.Name + "is not supported.");
                }
            }
            sBuilder.Remove(sBuilder.Length - 1, 1);
            sBuilder.Append(")");
            Console.WriteLine(sBuilder.ToString());
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = "@cmd";
            command.Parameters.AddWithValue("@cmd", sBuilder.ToString());
            command.CommandType = CommandType.Text;
            Console.WriteLine(command.Parameters["@cmd"].Value);
            command.ExecuteNonQuery();
            return true;
        }

        /// <summary>
        /// 将DataTable转换为IList
        /// </summary>
        /// <typeparam name="T">泛型，用于指定IList的数据类型</typeparam>
        /// <param name="table">数据表</param>
        /// <param name="name">要转换的数据名</param>
        /// <returns>IList或者null</returns>
        private IList<T> DataTableToList<T>(DataTable table, string name)
        {
            if (table == null || table.Columns.Count <= 0 || name == null || name.Length < 0)
                return null;
            int index = table.Columns.IndexOf(name);
            if (index < 0)
                return null;
            List<object> list = table.AsEnumerable().Select(r => r[name]).ToList();
            if (list == null || list.Count <= 0)
                return null;
            return list.OfType<T>().ToList();
        }
        #endregion

        #region 删除数据
        /// <summary>
        /// 删除表中数据
        /// <para>可以清空或删除表</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table"></param>
        /// <param name="column">删除的列名
        /// <para>默认值为空，表示所有列</para>
        /// </param>
        /// <param name="index">起始位置
        /// <para>0：从头开始</para>
        /// <para>-1：从尾开始</para>
        /// <para>默认值：0</para>
        /// </param>
        /// <param name="count">删除数量
        /// <para>-1：清空</para>
        /// <para>-2：删除</para>
        /// <para>默认值：-1</para>
        /// </param>
        /// <returns></returns>
        public virtual uint RemoveFromTable(string table, string column = "", int index = 0, int count = -1)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            if(count == -1)
            {

            }
            else if(count ==-2)
            {

            }
            else 
            {

            }
            return 1;
        }

        /// <summary>
        /// 删除表中符合字符串筛选条件的数据
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="column">删除的列名
        /// <para>如需删除多列，以英文逗号分割即可</para>
        /// </param>
        /// <param name="criteraColumn">筛选列名
        /// <para>如需筛选多列，以英文逗号分割即可</para>
        /// </param>
        /// <param name="way">筛选条件，包括
        /// <para>Contain</para>
        /// <para>NoContain</para>
        /// <para>StartWith</para>
        /// <para>EndWith</para>
        /// <para>None</para>
        /// </param>
        /// <param name="critera">筛选元素</param>
        /// <returns></returns>
        public virtual uint RemoveFromTable(string table, string column, string criteraColumn = "", TxtSelectCriteria way = TxtSelectCriteria.None, string critera = "")
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            return 1;
        }
        /// <summary>
        /// 删除表中符合数字筛选条件的数据
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="column">删除的列名
        /// <para>如需删除多列，以英文逗号分割即可</para>
        /// </param>
        /// <param name="criteraColumn">筛选列名
        /// <para>如需筛选多列，以英文逗号分割即可</para>
        /// </param>
        /// <param name="way">筛选条件，包括
        /// <para>Less Than</para>
        /// <para>Greater Than</para>
        /// <para>Less Than Or Equal</para>
        /// <para>Greater Than Or Equal</para>
        /// <para>Between</para>
        /// <para>None</para>
        /// </param>
        /// <param name="critera1">门限值</param>
        /// <param name="critera2">门限值</param>
        /// <returns></returns>
        protected virtual uint RemoveFromTable<T>(string table, string column, string criteraColumn = "", NumericSelectCriteria way = NumericSelectCriteria.None, T critera1 = default(T), T critera2 = default(T))
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            return 1;
        }
        #endregion

        #region 添加数据
        /// <summary>
        ///  在表中添加新数据
        ///  <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="column">数据所在的列名</param>
        /// <param name="value">数值</param>
        /// <returns></returns>
        public virtual int InsertToTable(string table, string column, object value)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            if(value == null)
                throw new System.ArgumentException("Parameter can not be null.", "value");
            ulong count = this.GetRowCountInTable(table);
            if (count >= MaxTableRowSize)
                throw new Exception(String.Format("Insert failed, current rows in {0} is {1} ,exceed the limit {2}.", table, count, MaxTableRowSize));
            return 1;
        }

        /// <summary>
        /// 在表中添加新数据
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <typeparam name="T">必须是结构体</typeparam>
        /// <param name="table">表名</param>
        /// <param name="column">数据所在的列名</param>
        /// <param name="values">数据，List</param>
        /// <param name="insertAsCan">是否插入
        /// <para>在表未达到最大行但仍不足插入所有values时，
        /// 此标志用于说明是否继续插入直到达到最大行或者取消本次插入</para>
        /// </param>
        /// <returns></returns>
        public virtual int InsertToTable<T>(string table, string column, List<T> values, bool insertAsCan) where T : struct
        {
            if (values == null || values.Count <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "values");
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            ulong count = this.GetRowCountInTable(table);
            ulong spaceLeft = MaxTableRowSize - count;
            if (spaceLeft <= 0)
                throw new Exception(String.Format("Insert failed, current rows in {0} is {1} ,exceed the limit {2}.", table, count, MaxTableRowSize));
            if (spaceLeft < (ulong)values.Count)
            {
                if (insertAsCan)
                {
                    //insert here
                    return (int)spaceLeft;
                }
                else
                {
                    throw new Exception(String.Format("Insert failed, current rows in {0} is {1} ,exceed the limit {2}.", table, count, MaxTableRowSize));
                }
            }
            return values.Count;
        }

        /// <summary>
        /// 在表中添加新数据
        /// <para>可用于一次添加多项数据</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="datatable">数据</param>
        /// <param name="insertAsCan">是否插入
        /// <para>在表未达到最大行但仍不足插入所有values时，
        /// 此标志用于说明是否继续插入直到达到最大行或者取消本次插入</para>
        /// </param>
        /// <returns></returns>
        public virtual int InsertToTable(string table, DataTable datatable, bool insertAsCan)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            return 500;
        }

        /// <summary>
        /// 在表中添加新数据
        /// <para>可用于一次添加多项数据</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.ArgumentOutOfRangeException</para>
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="rows">数据</param>
        /// <param name="insertAsCan">是否插入
        /// <para>在表未达到最大行但仍不足插入所有values时，
        /// 此标志用于说明是否继续插入直到达到最大行或者取消本次插入</para>
        /// </param>
        /// <returns></returns>
        public virtual int InsertToTable(string table, DataRow[] rows, bool insertAsCan)
        {
            if (table == null || table.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "table");
            CheckDatabaseConnection();
            if (!this.GetAllTables().Contains(table))
                throw new System.ArgumentOutOfRangeException("table", "Parameter does not exist.");
            return 500;
        }
        #endregion

        #region 读数据

        /// <summary>
        /// 从数据库中读取数据
        /// <para>用于读取一项数据</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <typeparam name="T">必须是结构体</typeparam>
        /// <param name="selection">查询语句
        /// <param name="column">需要查询的数据名称</param>
        /// <para>函数不判断查询语句的有效性</para>
        /// </param>
        /// <returns>IList</returns>
        public virtual IList<T> GetData<T>(string selection, string column) where T : struct
        {
            if (selection == null || selection.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "selection");
            if (column == null || column.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "column");
            CheckDatabaseConnection();
            DataTable table = this.GetData(selection);
            if (table.Rows.Count <= 0 || table.Columns.Count <= 0)
                return null;
            IList<T> rst = this.DataTableToList<T>(table, column);
            return rst;
        }

        /// <summary>
        /// <para>从数据库中读取数据</para>
        /// <para>用于读取多项数据</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <param name="selectionCmd">查询语句
        /// <para>函数不判断查询语句的合法性</para>
        /// </param>
        /// <returns>DataTable</returns>
        public virtual DataTable GetData(string selectionCmd)
        {
            if (selectionCmd == null || selectionCmd.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "selection");
            CheckDatabaseConnection();
            SqlCommand command = new SqlCommand();
            command.CommandText = "@cmd";
            command.Connection = sqlCon;
            command.Parameters.AddWithValue("@cmd", selectionCmd);
            DataTable t = new DataTable();
            t.BeginLoadData();
            t.Load(command.ExecuteReader());
            t.EndLoadData();
            return t;
        }

        /// <summary>
        /// 从数据库中读取数据
        /// <para>用于读取一项数据</para>
        /// <para>注意，本接口输入的是列名</para>
        /// <para>请区别与输入参数为sql语句的接口</para>
        /// </summary>
        /// <typeparam name="T">必须是结构体</typeparam>
        /// <param name="column">需要查询的数据列名称</param>
        /// <returns></returns>
        public virtual IList<T> GetData<T>(string column)
            where T : struct,IComparable
        {
            IList<T> t = new List<T>();
            t = this.GetData<T, T, T>(column);
            return t;
        }

        /// <summary>
        /// <para>从数据库中读取数据</para> 
        /// <para>用于读取数字型数据</para> 
        /// <para>T、U必须是结构体</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// <para>System.InvalidOperationException</para>
        /// <para>System.ArithmeticException</para>
        /// <example>例如，
        /// <code>
        /// IList rst = GetData("压力", "压力", DatabaseUtility.NumberSelectCriteria.GreaterThan, 1.0)
        /// </code>
        /// </example>
        /// </summary>
        /// <typeparam name="T">必须是结构体</typeparam>
        /// <typeparam name="U">必须是结构体</typeparam>
        /// <param name="column">需要查询的数据名称</param>
        /// <param name="criteriaColumn">作为筛选条件的数据名称，可以与查询名称一致</param>
        /// <param name="type">筛选条件，包括：
        /// <para>小于</para>
        /// <para>大于</para>
        /// <para>小于等于</para>
        /// <para>大于等于</para>
        /// <para>在范围内</para>
        /// <para>无</para>
        /// </param>
        /// <param name="gateLow">门限值
        /// <para>当筛选条件为“在范围内”时，表示下限</para>
        /// </param>
        /// <param name="gateHigh">门限值
        /// <para>当筛选条件为“在范围内”时，表示上限。其他条件时请忽略</para>
        /// </param>
        /// <returns> </returns>
        public virtual IList<T> GetData<T, U>(string column, string criteriaColumn = "", NumericSelectCriteria type = NumericSelectCriteria.None, 
            U gateLow = default(U), U gateHigh = default(U))
            where T : struct,IComparable
            where U : struct,IComparable
        {
            IList<T> t = new List<T>();
            t = this.GetData<T,U,U>(column,criteriaColumn,type,"",NumericSelectCriteria.None,gateLow,gateHigh);
            return t;
        }

        /// <summary>
        /// <para>从数据库中读取数据</para> 
        /// <para>用于读取数字型数据</para> 
        /// <para>T、U、V必须是结构体</para>
        /// <example>例如，
        /// <code>
        /// IList rst = GetData("压力", "压力", DatabaseUtility.NumberSelectCriteria.GreaterThan, , "温度", DatabaseUtility.NumberSelectCriteria.GreaterThan，1.0，10.0,1.0,23.0);
        /// </code>
        /// </example>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <typeparam name="T">必须是结构体</typeparam>
        /// <typeparam name="U">必须是结构体</typeparam>
        /// <typeparam name="V">必须是结构体</typeparam>
        /// <param name="column">需要查询的数据名称</param>
        /// <param name="criteriaColumn1">作为筛选条件的数据名称</param>
        /// <param name="type1">筛选条件1，包括：
        /// <para>小于</para>
        /// <para>大于</para>
        /// <para>小于等于</para>
        /// <para>大于等于</para>
        /// <para>在范围内</para>
        /// <para>无</para>
        /// </param>
        /// <param name="criteriaColumn2">作为筛选条件的数据名称</param>
        /// <param name="type2">筛选条件2，包括：
        /// <para>小于</para>
        /// <para>大于</para>
        /// <para>小于等于</para>
        /// <para>大于等于</para>
        /// <para>在范围内</para>
        /// <para>无</para>
        /// </param>
        /// <param name="gate1Low">筛选条件1的门限值</param>
        /// <param name="gate1High">筛选条件1的门限值</param>
        /// <param name="gate2Low">筛选条件2的门限值</param>
        /// <param name="gate2High">筛选条件2的门限值</param>
        /// <returns></returns>
        public virtual IList<T> GetData<T, U, V>(string column,
            string criteriaColumn1 = "", NumericSelectCriteria type1 = NumericSelectCriteria.None,
            string criteriaColumn2 = "", NumericSelectCriteria type2 = NumericSelectCriteria.None,
            U gate1Low = default(U), U gate1High = default(U),
            V gate2Low = default(V), V gate2High = default(V))
            where T : struct,IComparable
            where U : struct,IComparable
            where V : struct,IComparable
        {
            if (column == null || column.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "column");
            CheckDatabaseConnection();
            IList<T> tResult = new List<T>();
            //存储命令的若干变量
            StringBuilder sql = new StringBuilder("select @columnName from @tableName ");
            List<string> wheres = new List<string>();
            List<SqlParameter> listParameter = new List<SqlParameter>();
            listParameter.Add(new SqlParameter("@columnName", column));
            listParameter.Add(new SqlParameter("@tableName", "tb"));
            if (criteriaColumn1 != null && criteriaColumn1.Length > 0 && type1 != NumericSelectCriteria.None)
            {
                switch(type1)
                {
                    case NumericSelectCriteria.Between:
                        wheres.Add("@criteriaColumn1 BETWEEN @gate11 AND @gate12");
                        listParameter.Add(new SqlParameter("@criteriaColumn1", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate11", gate1Low));
                        listParameter.Add(new SqlParameter("@gate12", gate1High));
                        break;
                    case NumericSelectCriteria.GreaterThan:
                        wheres.Add("@criteriaColumn1 > @gate11");
                        listParameter.Add(new SqlParameter("@criteriaColumn1", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate11", gate1Low));
                        break;
                    case NumericSelectCriteria.GreaterThanOrEqual:
                        wheres.Add("@criteriaColumn1 >= @gate11");
                        listParameter.Add(new SqlParameter("@criteriaColumn1", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate11", gate1Low));
                        break;
                    case NumericSelectCriteria.LessThan:
                        wheres.Add("@criteriaColumn1 < @gate11");
                        listParameter.Add(new SqlParameter("@criteriaColumn1", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate11", gate1Low));
                        break;
                    case NumericSelectCriteria.LessThanOrEqual:
                    default:
                        wheres.Add("@criteriaColumn1 <= @gate11");
                        listParameter.Add(new SqlParameter("@criteriaColumn1", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate11", gate1Low));
                        break;
                }
            }
            if (criteriaColumn2 != null && criteriaColumn2.Length > 0 && type2 != NumericSelectCriteria.None)
            {
                switch (type2)
                {
                    case NumericSelectCriteria.Between:
                        wheres.Add("@criteriaColumn2 BETWEEN @gate21 AND @gate22");
                        listParameter.Add(new SqlParameter("@criteriaColumn2", criteriaColumn2));
                        listParameter.Add(new SqlParameter("@gate21", gate2Low));
                        listParameter.Add(new SqlParameter("@gate22", gate2High));
                        break;
                    case NumericSelectCriteria.GreaterThan:
                        wheres.Add("@criteriaColumn2 > @gate21");
                        listParameter.Add(new SqlParameter("@criteriaColumn2", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate21", gate2Low));
                        break;
                    case NumericSelectCriteria.GreaterThanOrEqual:
                        wheres.Add("@criteriaColumn2 >= @gate21");
                        listParameter.Add(new SqlParameter("@criteriaColumn2", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate21", gate2Low));
                        break;
                    case NumericSelectCriteria.LessThan:
                        wheres.Add("@criteriaColumn2 < @gate21");
                        listParameter.Add(new SqlParameter("@criteriaColumn2", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate21", gate2Low));
                        break;
                    case NumericSelectCriteria.LessThanOrEqual:
                    default:
                        wheres.Add("@criteriaColumn2 <= @gate21");
                        listParameter.Add(new SqlParameter("@criteriaColumn2", criteriaColumn1));
                        listParameter.Add(new SqlParameter("@gate21", gate2Low));
                        break;
                }
            }
            if(wheres.Count >0)
            {
                string wh = string.Join(" and ", wheres.ToArray());
                sql.Append(" where " + wh);
            }
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            command.CommandText = "@cmd";
            command.Parameters.AddWithValue("@cmd", sql.ToString());
            command.Parameters.AddRange(listParameter.ToArray());
            Console.WriteLine("sql: {0}", command.CommandText);
            DataTable table = new DataTable();
            table.BeginLoadData();
            table.Load(command.ExecuteReader());
            table.EndLoadData();
            tResult = this.DataTableToList<T>(table, column);
            return tResult;
        }

        /// <summary>
        /// <para>从数据库中读取数据</para> 
        /// <para>用于读取字符串数据</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <param name="column"></param>
        /// <param name="criteriaColumn"></param>
        /// <param name="type"></param>
        /// <param name="keyWord"></param>
        /// <returns></returns>
        public virtual IList<string> GetData(string column, string criteriaColumn = "", TxtSelectCriteria type = TxtSelectCriteria.None, string keyWord = "")
        {
            if (column == null || column.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "column");
            CheckDatabaseConnection();
            //生成sql语句
            string queryString = "select @columnName from @tableName ";
            SqlCommand command = new SqlCommand();
            command.Connection = sqlCon;
            if (criteriaColumn != null && criteriaColumn.Length > 0 && type != TxtSelectCriteria.None && keyWord != null && keyWord.Length > 0)
            {
                switch(type)
                {
                    case TxtSelectCriteria.Contain:
                        queryString += "where @criteriaColumn LIKE %@word" + /*keyWord + */"%";
                        break;
                    case TxtSelectCriteria.EndWith:
                        queryString += "where @criteriaColumn LIKE @word" + /*keyWord + */ "%";
                        break;
                    case TxtSelectCriteria.NoContain:
                        queryString += "where @criteriaColumn NOT LIKE %@word" +/*keyWord + */ "%";
                        break;
                    case TxtSelectCriteria.StartWith:
                    default:
                        queryString += "where @criteriaColumn NOT LIKE %@word" /*+ keyWord + */;
                        break;
                }
                command.CommandText = queryString;
                command.Parameters.AddWithValue("@criteriaColumn", criteriaColumn);
                command.Parameters.AddWithValue("@word", keyWord);
            }
            else
            {
                command.CommandText = queryString;
            }
            command.Parameters.AddWithValue("@columnName", column);
            command.Parameters.AddWithValue("@tableName", "tb");
            Console.WriteLine(command.CommandText);
            DataTable table = new DataTable();
            table.BeginLoadData();
            table.Load(command.ExecuteReader());
            table.EndLoadData();
            IList<string> t = this.DataTableToList<string>(table, column);
            return t;
        }

        /// <summary>
        /// <para>从数据库中读取数据</para> 
        /// <para>用于读取字符串数据</para> 
        /// <para>特别用于根据时间筛选信息</para>
        /// <para>Exceptions:</para> 
        /// <para>System.ArgumentException</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="column">数据名</param>
        /// <param name="criteriaColumn">筛选条件的数据名</param>
        /// <param name="type">筛选类型</param>
        /// <param name="gate1">门限值
        /// <para>当筛选条件为“在范围内”时，表示下限</para>
        /// </param>
        /// <param name="gate2">门限值
        /// <para>当筛选条件为“在范围内”时，表示上限。其他条件时请忽略</para>
        /// </param>
        /// <returns></returns>
        public virtual IList<string> GetData<T>(string column,
            string criteriaColumn = "", NumericSelectCriteria type = NumericSelectCriteria.None,
            T gate1 = default(T), T gate2 = default(T))
            where T : struct
        {
            if (column == null || column.Length <= 0)
                throw new System.ArgumentException("Parameter can not be null or empty.", "column");
            CheckDatabaseConnection();
            if(criteriaColumn == null || criteriaColumn.Length<=0 || type == NumericSelectCriteria.None)
            {
                IList<string> t = this.GetData(column, "",TxtSelectCriteria.None,"");
                return t;
            }
            Dictionary<string, Type> dict = this.GetColumnsAndTypeInTable("table");
            //Dictionary<string, Type> dict;
            //dict = new Dictionary<string, Type> { 
            //    {"integer ",typeof(int)},
            //    {"nchar(max)",typeof(string)},
            //    {"float",typeof(float)},
            //    {"datetime",typeof(DateTime)},
            //    {"datetim2e",typeof(DateTime)},
            //    };
            //判断筛选列是否为DateTime格式
            if (dict.ContainsKey(criteriaColumn) && dict[criteriaColumn] == typeof(DateTime))
            {
                //生成sql语句
                StringBuilder sql = new StringBuilder("select @columnName from @tableName ");
                List<string> wheres = new List<string>();
                List<SqlParameter> listParameter = new List<SqlParameter>();
                listParameter.Add(new SqlParameter("@columnName", column));
                listParameter.Add(new SqlParameter("@tableName", "tb"));
                switch(type)
                {
                    case NumericSelectCriteria.Between:
                        wheres.Add("@criteriaColumn BETWEEN @gate1 AND @gate2");
                        listParameter.Add(new SqlParameter("@criteriaColumn", criteriaColumn));
                        listParameter.Add(new SqlParameter("@gate1", gate1));
                        listParameter.Add(new SqlParameter("@gate2", gate2));
                        break;
                    case NumericSelectCriteria.GreaterThan:
                        wheres.Add("@criteriaColumn > @gate1");
                        listParameter.Add(new SqlParameter("@criteriaColumn", criteriaColumn));
                        listParameter.Add(new SqlParameter("@gate1", gate1));
                        break;
                    case NumericSelectCriteria.GreaterThanOrEqual:
                        wheres.Add("@criteriaColumn >= @gate1");
                         listParameter.Add(new SqlParameter("@criteriaColumn", criteriaColumn));
                        listParameter.Add(new SqlParameter("@gate1", gate1));
                        break;
                    case NumericSelectCriteria.LessThan:
                        wheres.Add("@criteriaColumn < @gate1");
                        listParameter.Add(new SqlParameter("@criteriaColumn", criteriaColumn));
                        listParameter.Add(new SqlParameter("@gate1", gate1));
                        break;
                    case NumericSelectCriteria.LessThanOrEqual:
                    default:
                        wheres.Add("@criteriaColumn <= @gate1");
                        listParameter.Add(new SqlParameter("@criteriaColumn", criteriaColumn));
                        listParameter.Add(new SqlParameter("@gate1", gate1));
                        break;
                }
                if (wheres.Count > 0)
                {
                    string wh = string.Join(" and ", wheres.ToArray());
                    sql.Append(" where " + wh);
                }
                SqlCommand command = new SqlCommand();
                command.Connection = sqlCon;
                command.CommandText = "@cmd";
                command.Parameters.AddWithValue("@cmd", sql.ToString());
                command.Parameters.AddRange(listParameter.ToArray());
                Console.WriteLine("paras:{0},{1},{2},{3}", gate1, gate2, command.Parameters["@cmd"].Value, command.Parameters["@tableName"].Value);
                DataTable table = new DataTable();
                table.BeginLoadData();
                table.Load(command.ExecuteReader());
                table.EndLoadData();
                return this.DataTableToList<string>(table,column);
            }
            throw new System.ArgumentException("column is not DateTime filed.", "criteriaColumn");
        }
        #endregion

        #region 事件
        /// <summary>
        /// 事件处理
        /// </summary>
        public event EventHandler<DbUtilityEvent> UtilityEvent;

        /// <summary>
        /// 事件参数
        /// <para>sealed</para>
        /// </summary>
        public sealed class DbUtilityEvent : EventArgs
        {
            /// <summary>
            /// 信息
            /// </summary>
            public string msg { get; set; }
            /// <summary>
            /// 时间戳
            /// </summary>
            public DateTime TimeReached { get; set; }
        }
        /// <summary>
        /// 触发事件
        /// </summary>
        /// <param name="e"></param>
        protected virtual void onUtilityEvent(DbUtilityEvent e)
        {
            if (UtilityEvent != null)
                UtilityEvent(this, e);
        }
        #endregion
    }

    /// <summary>
    /// 泛型比较类
    /// <para>用于验证筛选条件为Between时上下限的验证</para>
    /// <para>好像也可以不要验证，待确认</para>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Compare<T> where T : IComparable
    {
        /// <summary>
        /// 使用泛型实现的比较方法
        /// </summary>
        /// <param name="t1"></param>
        /// <param name="t2"></param>
        /// <returns></returns>
        public static bool compareGeneric(T t1, T t2)
        {
            if (t1.CompareTo(t2) >= 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
