﻿/*
 * Copyright 2018 Jan Tschada
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using MarkdownLog;
using OdbcManagement;
using OdbcShell.Properties;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;

namespace OdbcShell
{
    class Program
    {
        private static IEnumerable<string> GetAllConnectionStrings()
        {
            var odbcDrivers = ODBCManager.GetODBCDrivers();
            if (null == odbcDrivers)
            {
                yield break;
            }

            var userDatasourceNames = ODBCManager.GetUserDSNList();
            if (null == userDatasourceNames)
            {
                yield break;
            }

            foreach (var userDatasourceName in userDatasourceNames)
            {
                var driverPath = userDatasourceName.GetDSNDriverPath();
                foreach (var odbcDriver in odbcDrivers)
                {
                    if (0 == string.Compare(driverPath, odbcDriver.GetDriverDLL(), StringComparison.OrdinalIgnoreCase))
                    {
                        var properties = userDatasourceName.ToPropertyValues();
                        if (properties.ContainsKey(@"Servername")
                            && properties.ContainsKey(@"Database")
                            && properties.ContainsKey(@"Port"))
                        {
                            var driverName = odbcDriver.GetODBCDriverName();
                            var serverName = properties[@"Servername"];
                            var port = properties[@"Port"];
                            var databaseName = properties[@"Database"];
                            yield return string.Format(@"Driver={0};Server={1};Port={2};Database={3};Uid={4};Pwd={5})", driverName, serverName, port, databaseName);
                        }
                    }
                }
            }
        }

        private static string CreateQueryAllCommandText(string dataSource, string tableName)
        {
            if (0 == string.Compare(@"EXCEL", dataSource, StringComparison.OrdinalIgnoreCase))
            {
                return string.Format(@"SELECT * FROM [{0}];", tableName);
            }

            return string.Format(@"SELECT * FROM {0};", tableName);
        }

        private static void QueryDatabase(string connectionString)
        {
            using (var connection = new OdbcConnection(connectionString))
            {
                connection.Open();

                var dbMetaTables = connection.GetSchema(OdbcMetaDataCollectionNames.Tables);
                foreach (DataRow tableRow in dbMetaTables.Rows)
                {
                    var tableType = tableRow[@"TABLE_TYPE"] as string;
                    if (0 == string.Compare(@"TABLE", tableType, StringComparison.OrdinalIgnoreCase))
                    {
                        var tableName = tableRow[@"TABLE_NAME"] as string;
                        var queryAllCommandText = CreateQueryAllCommandText(connection.DataSource, tableName);
                        var selectCommand = new OdbcCommand(queryAllCommandText, connection);
                        using (var reader = selectCommand.ExecuteReader(CommandBehavior.SingleResult))
                        {
                            if (reader.HasRows)
                            {
                                var tableSchema = reader.GetSchemaTable();
                                var markdownColumns = new List<TableColumn>();
                                foreach (DataRow schemaRow in tableSchema.Rows)
                                {
                                    var columnName = schemaRow[@"ColumnName"] as string;
                                    markdownColumns.Add(new TableColumn { HeaderCell = new TableCell { Text = columnName } });
                                }

                                var markdownTable = new Table();
                                markdownTable.Columns = markdownColumns;

                                var fieldCount = reader.FieldCount;
                                var values = new object[fieldCount];
                                var maxShowCount = 5;
                                var markdownRows = new List<TableRow>(maxShowCount);
                                var recordCount = 0;
                                for (; reader.Read(); recordCount++)
                                {
                                    reader.GetValues(values);
                                    if (recordCount < maxShowCount)
                                    {
                                        var markdownCells = new List<TableCell>(fieldCount);
                                        foreach (var value in values)
                                        {
                                            markdownCells.Add(new TableCell { Text = Convert.ToString(value) });
                                        }
                                        markdownRows.Add(new TableRow { Cells = markdownCells });
                                    }
                                }

                                markdownTable.Rows = markdownRows;
                                Console.Write(markdownTable);

                                Console.WriteLine();
                                Console.WriteLine(@"{0} records read from {1}.", recordCount, tableName);
                                Console.WriteLine();
                            }
                        }
                    }
                }
            }
        }

        static void Main(string[] args)
        {
            // The user must have full access to the registry key
            // HKEY_LOCAL_MACHINE\SOFTWARE\ODBC
            // General error Unable to open registry key 'Temporary (volatile)
            var connectionString = Settings.Default.ConnectionString;
            QueryDatabase(connectionString);

            // Try to find Postgres
            connectionString = Environment.GetEnvironmentVariable(@"postgres.odbc");
            if (!string.IsNullOrEmpty(connectionString))
            {
                QueryDatabase(connectionString);
            }

            // Try to find all
            foreach (var registeredConnectionString in GetAllConnectionStrings())
            {
                QueryDatabase(registeredConnectionString);
            }
        }
    }
}
