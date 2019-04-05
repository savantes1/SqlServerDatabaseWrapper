using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;
using SqlServerDatabaseWrapper;

namespace ConsoleSimpleExample
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {

                // The data source to use when connecting (i.e. The SQL Server instance name or IP)
                var dataSource = "192.168.5.22";

                // The initial catalog to use once connected (i.e. The DB name)
                var initialCatalog = "AdventureWorks";

                var sqlWrapper = new SqlServerDBWrapper(dataSource, initialCatalog, true);

                var parameters = new List<SqlParameter>
                {
                    new SqlParameter("StartProductID", SqlDbType.Int) { Value = 717 },
                    new SqlParameter("CheckDate", SqlDbType.DateTime) { Value = DateTime.Now }
                };
                
                sqlWrapper.runSqlServerProcedure("dbo", "uspGetBillOfMaterials", parameters, reader =>
                {
                    //Write retrieved data to console
                    Console.WriteLine($"{reader.GetInt32(0)}   {reader.GetInt32(1)}   {reader.GetString(2)}   {reader.GetDecimal(3)}   {reader.GetDecimal(4)}");
                });
            }
            catch (SqlServerDBWrapperException sdbwe)
            {
                Console.WriteLine("SqlServerDBWrapperException Error:");
                Console.WriteLine(sdbwe.Message);
                Console.WriteLine($"Connection String: {sdbwe.SqlServerDBConnectionString}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine("General Error:");
                Console.WriteLine(e.Message);
            }

        }
    }
}
