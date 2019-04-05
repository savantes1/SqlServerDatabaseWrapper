/******************************************************************************************
*	SQL Server Database Wrapper Class
*
* Author:
*   Matthew Cervantes
*
* Purpose:
*   Encapsulates functionality required to communicate with
*   SQL Server database. Abstracts communication with database
*   resulting in cleaner code and a single point of interaction
*   with the database.
*
* Features:
*    1. Run SQL Query
*    2. Run SQL Insert
*    3. Run SQL Update
*    4. Run Oracle Procedure
*    5. Run Oracle Function
*    
*
* Instructions:
*   Create instance of class, creating a new database
*   connection string. Each member function allows for
*   different interactions with the database. You may
*   change the connection string without creating a new
*   instance of the class.
*
* Example Usage (Calling Stored Procedure):
* 
*   static void MyQueryFunction(SqlDataReader reader)
*   {    
*       while (reader.Read())
*       {
*           //Write retrieved data to console
*           Console.WriteLine(reader.GetInt32(0) + "   " + reader.GetInt32(1) +
*           "   " + reader.GetString(2) + "   " + reader.GetDecimal(3) + "   " + reader.GetDecimal(4));
*       }       
*   }
* 
* 
*   static void Main(string[] args)
*   {
*       try
*       {
*           //create instance of class and specify database connection parameters
*           SqlServerDBWrapper example = new SqlServerDBWrapper("192.168.5.22", "AdventureWorks", true);
*
*           //create first parameter that will be passed to the SQL Server stored procedure
*           SqlParameter param1 = new SqlParameter("StartProductID", SqlDbType.Int);
*           param1.Value = 717;
*
*           //create second parameter that will be passed to the SQL Server stored procedure
*           SqlParameter param2 = new SqlParameter("CheckDate", SqlDbType.DateTime);
*           param2.Value = DateTime.Now;
*
*           //make list of parameters
*           List<SqlParameter> parameters = new List<SqlParameter>();
*           parameters.Add(param1);
*           parameters.Add(param2);
*
*           //run SQL Server stored procedure
*           example.runSqlServerProcedure("dbo", "uspGetBillOfMaterials", parameters, MyQueryFunction);
*       }
*       catch (SqlServerDBWrapperException sdbwe)
*       {
*           Console.WriteLine("SqlServerDBWrapperException Error:");
*           Console.WriteLine(sdbwe.Message);
*           Console.WriteLine("Connection String: " + sdbwe.SqlServerDBConnectionString);
*       }
*       catch (Exception ex)
*       {
*           Console.WriteLine("General Error:");
*           Console.WriteLine(ex.Message);
*       }
*   }
*            
*      
******************************************************************************************/

namespace SqlServerDatabaseWrapper
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Xml.Linq;
    using System.Xml;

    /// <summary>
    /// Custom Sql Server Database Wrapper Exception Class
    /// </summary>
    [Serializable]
    public class SqlServerDBWrapperException : Exception
    {
        private string _sqlServerDBConnectionString;
        private SqlServerDBWrapperErrorType _errorType;

        /// <summary>
        /// SqlServerDBWrapper error type
        /// </summary>
        public enum SqlServerDBWrapperErrorType
        {
            /// <summary>
            /// Error is related to connection string
            /// </summary>
            Connection,

            /// <summary>
            /// Error is related to SQL-based functionality
            /// </summary>
            SQL_Error,

            /// <summary>
            /// Error is related to T-SQL based functionality
            /// </summary>
            T_SQL_Error
        }

        /// <summary>
        /// The connection string used when the exception occured
        /// </summary>
        public string SqlServerDBConnectionString
        {
            get { return _sqlServerDBConnectionString; }
        }

        /// <summary>
        /// The type of error
        /// </summary>
        public SqlServerDBWrapperErrorType ErrorType
        {
            get { return _errorType; }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        /// <param name="connectionString">The Sql Server connection string being used when the exception was thrown.</param>
        /// <param name="errorType">The type of error that occurred</param>
        public SqlServerDBWrapperException(string message, string connectionString, SqlServerDBWrapperErrorType errorType)
            : base(message)
        {
            _sqlServerDBConnectionString = connectionString;
            _errorType = errorType;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="message">The message that describes the error</param>
        /// <param name="innerExeption">The exception that is the cause of the current exception, or a null reference
        /// (Nothing in Visual Basic) if no inner exception is specified.</param>
        /// <param name="connectionString">The Sql Server connection string being used when the exception was thrown.</param>
        /// <param name="errorType">The type of error that occurred</param>
        public SqlServerDBWrapperException(string message, Exception innerExeption, string connectionString, SqlServerDBWrapperErrorType errorType)
            : base(message, innerExeption)
        {
            _sqlServerDBConnectionString = connectionString;
            _errorType = errorType;
        }

        /// <summary>
        /// Initializes a new instance of the System.Exception class with serialized data.
        /// </summary>
        /// <param name="info">The System.Runtime.Serialization.SerializationInfo that holds the
        /// object data about the exception being thrown.</param>
        /// <param name="context">The System.Runtime.Serialization.StreamContext that contains 
        /// contextual information about the source or destination.</param>
        protected SqlServerDBWrapperException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }


    /// <summary>
    /// Provides wrapper functions for common Sql Server database tasks
    /// </summary>
    public sealed class SqlServerDBWrapper
    {
        private string _connectionDataSource;
        private string _connectionId;
        private string _connectionPassword;
        private string _connectionInitialCatalog;
        private string _connectionIntegratedSecurity;

        private bool _validConnection;

        /// <summary>
        /// Query delegate function to use when reading queried data
        /// </summary>
        /// <param name="reader"></param>
        public delegate void QueryFunction(SqlDataReader reader);

        /// <summary>
        /// Constructor using username and password
        /// </summary>
        /// <param name="connectionDataSource">The data source to use when connecting</param>
        /// <param name="initialCatalog">The initial catalog to use once connected</param>
        /// <param name="connectionId">The id of the user to use when connecting</param>
        /// <param name="connectionPassword">The password for the user who is connecting</param>
        public SqlServerDBWrapper(string connectionDataSource, string initialCatalog, string connectionId, string connectionPassword)
        {
            setConnectionParams(connectionDataSource, connectionId, connectionPassword, initialCatalog, false);
            DefualtCommandTimeout = 30;
        }

        /// <summary>
        /// Constructor using integrated security
        /// </summary>
        /// <param name="connectionDataSource">The data source to use when connecting</param>
        /// <param name="initialCatalog">The initial catalog to use once connected</param>
        /// <param name="integratedSecurity">Used to determine if integrated security should be used when connecting</param>
        public SqlServerDBWrapper(string connectionDataSource, string initialCatalog, bool integratedSecurity)
        {
            setConnectionParams(connectionDataSource, string.Empty, string.Empty, initialCatalog, integratedSecurity);
            DefualtCommandTimeout = 30;
        }

        /// <summary>
        /// Sets the connection string parameters and then tests the connection
        /// </summary>
        /// <param name="connectionDataSource">The data source to use when connecting</param>
        /// <param name="connectionId">The id of the user to use when connecting</param>
        /// <param name="connectionPassword">The password for the user who is connecting</param>
        /// <param name="initialCatalog">The initial catalog to use once connected</param>
        /// <param name="integratedSecurity">True if using integrated security, otherwise false</param>
        public void setConnectionParams(string connectionDataSource, string connectionId, string connectionPassword, string initialCatalog, bool integratedSecurity)
        {
            _connectionDataSource = connectionDataSource;
            _connectionId = connectionId;
            _connectionPassword = connectionPassword;
            _connectionInitialCatalog = initialCatalog;
            _connectionIntegratedSecurity = integratedSecurity.ToString();

            testConnection();
        }

        /// <summary>
        /// Returns the string passed to the database to establish connection
        /// </summary>
        public string ConnectionString
        {
            get
            {
                string returnVal = "Data Source=" + _connectionDataSource + ";Initial Catalog=" +
                    _connectionInitialCatalog + ";Integrated Security=" + _connectionIntegratedSecurity;

                if (!string.IsNullOrEmpty(_connectionId))
                {
                    returnVal += ";User Id=" + _connectionId + ";Password=" + _connectionPassword;
                }

                return returnVal;
            }
        }

        /// <summary>
        /// The default command timeout used for all queries. This value can be overridden
        /// by using the "commandTimeout" parameter on several methods. 
        /// </summary>
        public int DefualtCommandTimeout { get; set; }

        /// <summary>
        /// Tests the current connection string by trying to open and close the connection
        /// </summary>
        private void testConnection()
        {
            _validConnection = true;

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                }
            }
            catch (Exception ex)
            {
                _validConnection = false;
                throw new SqlServerDBWrapperException(ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }
        }



        /// <summary>
        /// Executes the specified SQL query.
        /// </summary>
        /// <param name="queryString">The query string. (Note: replacement parameters must be denoted in the query string
        /// using the format @Param[num] where num >= 1 (ex. select ID from PERSON where FIRST_NAME = @Param1 and LAST_NAME = @Param2))</param>
        /// <param name="queryFunction">The function that will be called to handle the query results.</param>
        /// <param name="parameters">The parameters associated with each @Param[num] in the query string. The first parameter correlates
        /// to @Param1, the second with @Param2 and so on.</param>
        public void runSqlServerQuery(string queryString, QueryFunction queryFunction, params string[] parameters)
        {
            runSqlServerQuery(queryString, queryFunction, DefualtCommandTimeout, parameters);
        }


        /// <summary>
        /// Executes the specified SQL query.
        /// </summary>
        /// <param name="queryString">The query string. (Note: replacement parameters must be denoted in the query string
        /// using the format @Param[num] where num >= 1 (ex. select ID from PERSON where FIRST_NAME = @Param1 and LAST_NAME = @Param2))</param>
        /// <param name="queryFunction">The function that will be called to handle the query results.</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">The parameters associated with each @Param[num] in the query string. The first parameter correlates
        /// to @Param1, the second with @Param2 and so on.</param>
        public void runSqlServerQuery(string queryString, QueryFunction queryFunction, int commandTimeout, params string[] parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (SqlCommand cmd = new SqlCommand(queryString, conn))
                    {
                        cmd.CommandTimeout = commandTimeout;

                        for (int i = 1; i <= parameters.Length; i++)
                        {
                            cmd.Parameters.Add("Param" + i.ToString(), SqlDbType.VarChar).Value = parameters[i - 1];
                        }

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            queryFunction(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerQuery Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified SQL query expecting an XML result.
        /// </summary>
        /// <param name="queryString">The query string. (Note: replacement parameters must be denoted in the query string
        /// using the format @Param[num] where num >= 1 (ex. select ID from PERSON where FIRST_NAME = @Param1 and LAST_NAME = @Param2))</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">The parameters associated with each @Param[num] in the query string. The first parameter correlates
        /// to @Param1, the second with @Param2 and so on.</param>
        /// <returns>The results of the SQL query represented as an XElement type</returns>
        public XElement runSqlServerQueryXML(string queryString, int commandTimeout, params string[] parameters)
        {
            //if query string doesn't contain the "FOR XML" clause, throw exception
            if (!queryString.ToUpper().Contains("FOR XML"))
            {
                throw new SqlServerDBWrapperException("Query String Does Not Contain \"FOR XML\" Clause", 
                    ConnectionString, SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }

            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (SqlCommand cmd = new SqlCommand(queryString, conn))
                    {
                        cmd.CommandTimeout = commandTimeout;

                        for (int i = 1; i <= parameters.Length; i++)
                        {
                            cmd.Parameters.Add("Param" + i.ToString(), SqlDbType.VarChar).Value = parameters[i - 1];
                        }

                        using (XmlReader reader = cmd.ExecuteXmlReader())
                        {
                            if (reader.Read())
                            {
                                return XElement.Load(reader);
                            }
                            else
                            {
                                return null;
                            }
                        }
                        
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerQueryXML Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.SQL_Error);
            }
        }

        /// <summary>
        /// Executes the specified SQL query expecting an XML result.
        /// </summary>
        /// <param name="queryString">The query string. (Note: replacement parameters must be denoted in the query string
        /// using the format @Param[num] where num >= 1 (ex. select ID from PERSON where FIRST_NAME = @Param1 and LAST_NAME = @Param2))</param>
        /// <param name="parameters">The parameters associated with each @Param[num] in the query string. The first parameter correlates
        /// to @Param1, the second with @Param2 and so on.</param>
        /// <returns>The results of the SQL query represented as an XElement type</returns>
        public XElement runSqlServerQueryXML(string queryString, params string[] parameters)
        {
            return runSqlServerQueryXML(queryString, DefualtCommandTimeout, parameters);
        }




        /// <summary>
        /// Executes the specified SQL insert
        /// </summary>
        /// <param name="insertString">The insert string</param>
        /// <param name="parameters">The parameters that correlate to the specified replacement parameters 
        /// in the insert string</param>
        public void runSqlServerInsert(string insertString, params SqlParameter[] parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (SqlCommand cmd = new SqlCommand(insertString, conn))
                    {
                        foreach (SqlParameter param in parameters)
                        {
                            cmd.Parameters.Add(param);
                        }

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerInsert Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.SQL_Error);
            }
        }

        /// <summary>
        /// Executes the specified SQL update
        /// </summary>
        /// <param name="updateString">The update string</param>
        /// <param name="parameters">The parameters that correlate to the specified replacement parameters
        /// in the update string</param>
        public void runSqlServerUpdate(string updateString, params SqlParameter[] parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    using (SqlCommand cmd = new SqlCommand(updateString, conn))
                    {
                        foreach (SqlParameter param in parameters)
                        {
                            cmd.Parameters.Add(param);
                        }

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerUpdate Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.SQL_Error);
            }
        }



        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, int commandTimeout, List<SqlParameter> parameters, QueryFunction queryFunction)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        //add all the passed in parameters
                        foreach (SqlParameter sp in parameters)
                        {
                            cmd.Parameters.Add(sp);
                        }

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            queryFunction(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedure Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, List<SqlParameter> parameters, QueryFunction queryFunction)
        {
            runSqlServerProcedure(procedureSchemaName, procedureName, DefualtCommandTimeout, parameters, queryFunction);
        }




        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, int commandTimeout, List<SqlParameter> parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        //add all the passed in parameters
                        foreach (SqlParameter sp in parameters)
                        {
                            cmd.Parameters.Add(sp);
                        }

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedure Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, List<SqlParameter> parameters)
        {
            runSqlServerProcedure(procedureSchemaName, procedureName, DefualtCommandTimeout, parameters);
        }



        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, int commandTimeout, QueryFunction queryFunction)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            queryFunction(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedure Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, QueryFunction queryFunction)
        {
            runSqlServerProcedure(procedureSchemaName, procedureName, DefualtCommandTimeout, queryFunction);
        }




        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName, int commandTimeout)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedure Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server procedure
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        public void runSqlServerProcedure(string procedureSchemaName, string procedureName)
        {
            runSqlServerProcedure(procedureSchemaName, procedureName, DefualtCommandTimeout);
        }



        /// <summary>
        /// Executes the specified Sql Server procedure expecting an XML result
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        /// <returns>The results of the stored procedure represented as an XElement type</returns>
        public XElement runSqlServerProcedureXML(string procedureSchemaName, string procedureName, int commandTimeout, List<SqlParameter> parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        //add all the passed in parameters
                        foreach (SqlParameter sp in parameters)
                        {
                            cmd.Parameters.Add(sp);
                        }

                        using (XmlReader reader = cmd.ExecuteXmlReader())
                        {
                            if (reader.Read())
                            {
                                return XElement.Load(reader);
                            }
                            else
                            {
                                return null;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedureXML Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server procedure expecting an XML result
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the procedure. The objects in the
        /// list can be referenced after the call to allow for returning values from out or in/out parameters to the procedure.</param>
        /// <returns>The results of the stored procedure represented as an XElement type</returns>
        public XElement runSqlServerProcedureXML(string procedureSchemaName, string procedureName, List<SqlParameter> parameters)
        {
            return runSqlServerProcedureXML(procedureSchemaName, procedureName, DefualtCommandTimeout, parameters);
        }



        /// <summary>
        /// Executes the specified Sql Server procedure expecting an XML result
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <returns>The results of the stored procedure represented as an XElement type</returns>
        public XElement runSqlServerProcedureXML(string procedureSchemaName, string procedureName, int commandTimeout)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    procedureSchemaName = procedureSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + procedureSchemaName + "]." + procedureName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        using (XmlReader reader = cmd.ExecuteXmlReader())
                        {
                            if (reader.Read())
                            {
                                return XElement.Load(reader);
                            }
                            else
                            {
                                return null;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerProcedureXML Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }



        /// <summary>
        /// Executes the specified Sql Server procedure expecting an XML result
        /// </summary>
        /// <param name="procedureSchemaName">The name of the schema which this procedure belongs</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <returns>The results of the stored procedure represented as an XElement type</returns>
        public XElement runSqlServerProcedureXML(string procedureSchemaName, string procedureName)
        {
            return runSqlServerProcedureXML(procedureSchemaName, procedureName, DefualtCommandTimeout);
        }





        /// <summary>
        /// Executes the specified Sql Server function and returns a value
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema which this function belongs</param>
        /// <param name="functionName">The name of the function</param>
        /// <param name="returnType">The return type of the function</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the function.</param>
        /// <returns>The result of the specified Sql Server function</returns>
        public object runSqlServerFunction(string functionSchemaName, string functionName, SqlDbType returnType, int commandTimeout, params SqlParameter[] parameters)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    functionSchemaName = functionSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + functionSchemaName + "]." + functionName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        //create the return parameter
                        SqlParameter returnParam = new SqlParameter("MC_FUNCTION_RETURN_PARAMETER", returnType);
                        returnParam.Direction = ParameterDirection.ReturnValue;
                        cmd.Parameters.Add(returnParam);

                        //add all the passed in parameters
                        foreach (SqlParameter sp in parameters)
                        {
                            cmd.Parameters.Add(sp);
                        }

                        cmd.ExecuteNonQuery();

                        return cmd.Parameters["MC_FUNCTION_RETURN_PARAMETER"].Value;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerFunction Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }


        /// <summary>
        /// Executes the specified Sql Server function and returns a value
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema which this function belongs</param>
        /// <param name="functionName">The name of the function</param>
        /// <param name="returnType">The return type of the function</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the function.</param>
        /// <returns>The result of the specified Sql Server function</returns>
        public object runSqlServerFunction(string functionSchemaName, string functionName, SqlDbType returnType, params SqlParameter[] parameters)
        {
            return runSqlServerFunction(functionSchemaName, functionName, returnType, DefualtCommandTimeout, parameters);
        }


        /// <summary>
        /// Executes the specified Sql Server function and returns a value
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema which this function belongs</param>
        /// <param name="functionName">The name of the function</param>
        /// <param name="returnType">The return type of the function</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <returns>The result of the specified Sql Server function</returns>
        public object runSqlServerFunction(string functionSchemaName, string functionName, SqlDbType returnType, int commandTimeout)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    functionSchemaName = functionSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    using (SqlCommand cmd = new SqlCommand("[" + functionSchemaName + "]." + functionName, conn))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.CommandTimeout = commandTimeout;

                        //create the return parameter
                        SqlParameter returnParam = new SqlParameter("MC_FUNCTION_RETURN_PARAMETER", returnType);
                        returnParam.Direction = ParameterDirection.ReturnValue;
                        cmd.Parameters.Add(returnParam);

                        cmd.ExecuteNonQuery();

                        return cmd.Parameters["MC_FUNCTION_RETURN_PARAMETER"].Value;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerFunction Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.T_SQL_Error);
            }
        }



        /// <summary>
        /// Executes the specified Sql Server function and returns a value
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema which this function belongs</param>
        /// <param name="functionName">The name of the function</param>
        /// <param name="returnType">The return type of the function</param>
        /// <returns>The result of the specified Sql Server function</returns>
        public object runSqlServerFunction(string functionSchemaName, string functionName, SqlDbType returnType)
        {
            return runSqlServerFunction(functionSchemaName, functionName, returnType, DefualtCommandTimeout);
        }


        /// <summary>
        /// Executes the specified Sql Server table-valued function.
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema to which this function belongs</param>
        /// <param name="functionName">The name of the stored function</param>
        /// <param name="commandTimeout">The time to wait before terminating the command attempt and generating an error.</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the function (Note: The 
        /// order of the parameters in the list is order that the parameters will be passed to the function.
        /// Hence, if the parameter order is incorrect in the parameter list, incorrect results may occur).</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerTableValuedFunction(string functionSchemaName, string functionName, int commandTimeout, List<SqlParameter> parameters, QueryFunction queryFunction)
        {
            //if connection has already failed and hasn't changed, throw exception
            if (!_validConnection)
            {
                throw new SqlServerDBWrapperException("Invalid Connection String", ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.Connection);
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();

                    //Get rid of brackets if the user already put them in
                    functionSchemaName = functionSchemaName.Replace("[", string.Empty).Replace("]", string.Empty);

                    
                    StringBuilder query = new StringBuilder();
                    for (int i = 1; i <= parameters.Count; i++)
                    {
                        bool needsQuotes = false;
                        bool unicode = false;

                        // Determine if parameter needs quotes when setting sql server variable
                        // and if the parameter value is unicode
                        switch (parameters[i - 1].SqlDbType)
                        {
                            case SqlDbType.VarChar:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Xml:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Date:
                                needsQuotes = true;
                                break;
                            case SqlDbType.DateTime:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Char:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Text:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Time:
                                needsQuotes = true;
                                break;
                            case SqlDbType.NChar:
                                needsQuotes = true;
                                unicode = true;
                                break;
                            case SqlDbType.NText:
                                needsQuotes = true;
                                unicode = true;
                                break;
                            case SqlDbType.NVarChar:
                                needsQuotes = true;
                                unicode = true;
                                break;
                            case SqlDbType.Binary:
                                needsQuotes = true;
                                break;
                            case SqlDbType.DateTime2:
                                needsQuotes = true;
                                break;
                            case SqlDbType.SmallDateTime:
                                needsQuotes = true;
                                break;
                            case SqlDbType.Timestamp:
                                needsQuotes = true;
                                break;
                        }

                        //Build the query string
                        query.Append("declare @param" + i.ToString() + " " + parameters[i-1].SqlDbType.ToString() + 
                            (parameters[i-1].SqlDbType.Equals(SqlDbType.VarChar) || parameters[i-1].SqlDbType.Equals(SqlDbType.NVarChar) ? "(max)" : string.Empty) +
                            " = " + (unicode ? "N" : string.Empty) + (needsQuotes ? "'" : string.Empty) + 
                            parameters[i-1].SqlValue + (needsQuotes ? "'" : string.Empty) + ";");
                    }

                    query.Append("select * from [" + functionSchemaName + "]." + functionName + "(");

                    for (int i = 1; i <= parameters.Count; i++)
                    {
                        if (i == 1)
                            query.Append("@param" + i.ToString());
                        else
                            query.Append(",@param" + i.ToString());
                    }

                    query.Append(");");

                    using (SqlCommand cmd = new SqlCommand(query.ToString(), conn))
                    {
                        cmd.CommandTimeout = commandTimeout;

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            queryFunction(reader);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new SqlServerDBWrapperException("runSqlServerTableValuedFunction Error: " + ex.Message, ConnectionString,
                    SqlServerDBWrapperException.SqlServerDBWrapperErrorType.SQL_Error);
            }

        }


        /// <summary>
        /// Executes the specified Sql Server table-valued function.
        /// </summary>
        /// <param name="functionSchemaName">The name of the schema to which this function belongs</param>
        /// <param name="functionName">The name of the stored function</param>
        /// <param name="parameters">A list of SqlParameter parameters to pass to the function (Note: The 
        /// order of the parameters in the list is order that the parameters will be passed to the function.
        /// Hence, if the parameter order is incorrect in the parameter list, incorrect results may occur).</param>
        /// <param name="queryFunction">The function that will be called to handle the procedure query results.</param>
        public void runSqlServerTableValuedFunction(string functionSchemaName, string functionName, List<SqlParameter> parameters, QueryFunction queryFunction)
        {
            runSqlServerTableValuedFunction(functionSchemaName, functionName, DefualtCommandTimeout, parameters, queryFunction);
        }


    }
}
