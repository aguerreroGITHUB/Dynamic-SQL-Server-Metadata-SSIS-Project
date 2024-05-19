#region Namespaces
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Runtime.InteropServices;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Configuration;
#endregion

namespace SynchronizationByDate
{
    [Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
    public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    {
        public void Main()
        {
            #region SSIS Reads + Initializations 
            // Read SSIS variables
            string Prefix = (string)Dts.Variables["Prefix"].Value;
            string Datamart = (string)Dts.Variables["Datamart"].Value;
            string StartDate = (string)Dts.Variables["StartDate"].Value;

            string QueryColumns = (string)Dts.Variables["SourceColumns"].Value;
            string QueryColumnsPK = (string)Dts.Variables["SourceColumnsPK"].Value;

            string ConnectionPathSource = (string)Dts.Variables["ConnectionPathSource"].Value;
            string ConnectionPathDestination = (string)Dts.Variables["ConnectionPathDestination"].Value;

            // Variable initialization
            bool skip = true;
            DataTable Columns = new DataTable();
            DataTable ColumnsPK = new DataTable();

            // Connections to the databases
            SqlConnection source = new SqlConnection(ConnectionPathSource);
            source.Open();
            SqlConnection destination = new SqlConnection(ConnectionPathDestination);
            destination.Open();
            #endregion

            #region Load columns
            Columns.Load(ExtractData(QueryColumns, source));
            ColumnsPK.Load(ExtractData(QueryColumnsPK, source));
            #endregion

            #region Synchronization
            try
            {
                // Generate Source reads
                Dts.Events.FireInformation(0, "Synchronization " + DateTime.Now.TimeOfDay, "Generating reads...", string.Empty, 0, ref skip);
                List<string> Tables = FindTables(Columns, Prefix, Datamart, source);
                string SynchronizationSource = GenerateReadSource(Columns, ColumnsPK, Tables, StartDate);

                SqlCommand SynchronizationCommand = new SqlCommand(SynchronizationSource, source);
                SynchronizationCommand.CommandTimeout = 600; // 10 minutes
                SqlDataReader SourceQuery = SynchronizationCommand.ExecuteReader();
                Dts.Events.FireInformation(0, "Synchronization " + DateTime.Now.TimeOfDay, "Data read completed", string.Empty, 0, ref skip);

                // Delete the pre-configured period
                using (SqlCommand Delete = new SqlCommand("Delete from dbo.[" + Datamart + "] where [$SystemModifiedAt] >= '" + StartDate + "';", destination))
                {
                    Delete.ExecuteNonQuery();
                    Dts.Events.FireInformation(0, "Synchronization " + DateTime.Now.TimeOfDay, "Destination table prepared", string.Empty, 0, ref skip);
                }

                // Dump the new period
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destination))
                {
                    bulkCopy.DestinationTableName = "dbo.[" + Datamart + "]";

                    // Column mappings
                    foreach (DataRow Column in ColumnsPK.Rows)
                    {
                        bulkCopy.ColumnMappings.Add("[" + Column[0] + "]", "[" + Column[0] + "]");

                    }

                    foreach (DataRow Column in Columns.Rows)
                    {
                        bulkCopy.ColumnMappings.Add("[" + Column[0] + "]", "[" + Column[0] + "]");

                    }

                    // Dump configuration, no timeout and notify every half a million rows.
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.BatchSize = (int)(2097152 / Columns.Columns.Count);
                    bulkCopy.NotifyAfter = 250000;
                    bulkCopy.SqlRowsCopied += (sender, e) =>
                    {
                        Dts.Events.FireInformation(0, "Synchronization " + DateTime.Now.TimeOfDay, $"{e.RowsCopied} inserted.", string.Empty, 0, ref skip);
                    };
                    // Dump
                    bulkCopy.WriteToServer(SourceQuery);
                    Dts.Events.FireInformation(0, "Synchronization " + DateTime.Now.TimeOfDay, "Successfully completed.", string.Empty, 0, ref skip);
                    Dts.TaskResult = (int)ScriptResults.Success;
                }
            }
            catch (Exception ex)
            {
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, ex.Message, String.Empty, 0);
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, ex.StackTrace, String.Empty, 0);
                Dts.TaskResult = (int)ScriptResults.Failure;
            }

            // Close connections
            source.Close();
            destination.Close();
            #endregion
        }

        #region Auxiliary Functions
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        private SqlDataReader ExtractData(string Query, SqlConnection Connection)
        {
            SqlCommand command = new SqlCommand(Query, Connection);
            command.CommandTimeout = 300; // 5 minutes
            SqlDataReader sqlData = command.ExecuteReader();
            return sqlData;
        }
        private List<string> FindTables(DataTable Columns, string Prefix, string Datamart, SqlConnection DB)
        {
            List<string> Tables = new List<string>();
            string QuerySuffixes = @"SELECT Suffix
                FROM [dbo].[TablesAndColumnsSource] t1
                WHERE Datamart = '" + Datamart + @"'
                and Prefix = '" + Prefix + @"'
                and IsPKField = 0
                GROUP BY Suffix";

            SqlCommand SQLInstruction = new SqlCommand(QuerySuffixes, DB);
            SQLInstruction.CommandTimeout = 300; // 5 minutes

            SqlDataReader Suffixes = SQLInstruction.ExecuteReader();
            while (Suffixes.Read())
            {
                if (Suffixes["Suffix"].ToString().Substring(0, 4) == "$437")
                {// The suffix of the main tables starts with $437
                    Tables.Insert(0, Prefix + Datamart + Suffixes["Suffix"].ToString());
                }
                else
                {
                    Tables.Add(Prefix + Datamart + Suffixes["Suffix"].ToString());
                }
            }
            Suffixes.Close();
            return Tables;
        }
        private string GenerateReadSource(DataTable Columns, DataTable ColumnsPK, List<string> Tables, string StartDate)
        {
            string ReadSource = "Select t1.timestamp,";

            foreach (DataRow Column in ColumnsPK.Rows)
            {// Add PK fields
                if (Column[0].ToString() != "timestamp")
                {
                    ReadSource += "\n t1.[" + Column[0] + "],";
                }
            }

            foreach (DataRow Column in Columns.Rows)
            {// Add the rest of the columns
                if (Column[0].ToString() != "timestamp")
                {
                    ReadSource += + "\n [" + Column[0] + "],";
                }
            }

            ReadSource = ReadSource.EndsWith(",") ? ReadSource.Remove(ReadSource.Length - 1) : ReadSource;

            for (int i = 0; i < Tables.Count; i++)
            {
                if (i == 0)
                {// The first table is FROM, not INNER JOIN.
                    ReadSource = ReadSource + "\nFROM dbo.[" + Tables[i] + "] t1";
                }
                else
                {
                    ReadSource = ReadSource + "\n  INNER JOIN dbo.[" + Tables[i] + "] t" + (i + 1);
                    foreach (DataRow PK in ColumnsPK.Rows)
                    {
                        if (PK[0].ToString() != "timestamp")
                        {
                            if (ReadSource.EndsWith(" t" + (i + 1)))
                            {
                                ReadSource = ReadSource + "\n    ON  t1.[" + PK[0]
                                         + "] = t" + (i + 1) + ".[" + PK[0] + "]";
                            }
                            else
                            {
                                ReadSource = ReadSource + "\n    AND t1.[" + PK[0]
                                         + "] = t" + (i + 1) + ".[" + PK[0] + "]";
                            }
                        }

                    }
                }
            }

            return ReadSource + "\nWHERE [$SystemModifiedAt] >= '" + StartDate + "'";
        }
        #endregion

    }
}
