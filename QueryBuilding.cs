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

namespace QueryBuilder
{
    [Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
    public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    {
        public void Main()
        {
            #region SSIS Readings + Initializations 
            string SourceColumnsQuery = (string)Dts.Variables["SourceColumns"].Value;
            string DestinationColumnsQuery = (string)Dts.Variables["DestinationColumns"].Value;
            string SourcePKColumnsQuery = (string)Dts.Variables["SourceColumnsPK"].Value;

            string StartDate = (string)Dts.Variables["StartDate"].Value;
            string Prefix = (string)Dts.Variables["Prefix"].Value;
            string Datamart = (string)Dts.Variables["Datamart"].Value;
            string SourcePath = (string)Dts.Variables["SourcePath"].Value;
            string DestinationPath = (string)Dts.Variables["DestinationPath"].Value;

            bool skip = true;

            DataTable SourceColumns = new DataTable();
            DataTable PKColumns = new DataTable();
            DataTable DestinationColumns = new DataTable();

            // Database connections
            SqlConnection sourceConnection = new SqlConnection(SourcePath);
            sourceConnection.Open();
            SqlConnection destinationConnection = new SqlConnection(DestinationPath);
            destinationConnection.Open();
            #endregion

            #region Data loading
            SourceColumns.Load(ExtractData(SourceColumnsQuery, sourceConnection));
            PKColumns.Load(ExtractData(SourcePKColumnsQuery, sourceConnection));
            DestinationColumns.Load(ExtractData(DestinationColumnsQuery, destinationConnection));
            #endregion

            #region Script logic
            try
            {
                #region Pre-assignments
                Dts.Events.FireInformation(0, "Generation " + DateTime.Now.TimeOfDay, "Building PKColumnList...", string.Empty, 0, ref skip);
                List<string> PKColumnList = ColumnToList(PKColumns, 0);

                Dts.Events.FireInformation(0, "Generation " + DateTime.Now.TimeOfDay, "Building NewColumns...", string.Empty, 0, ref skip);
                List<(string Column, string DataType, bool DifferentMetadata)> NewColumns = FindNewColumns(SourceColumns, DestinationColumns);

                Dts.Events.FireInformation(0, "Generation " + DateTime.Now.TimeOfDay, "Building DeletedColumns...", string.Empty, 0, ref skip);
                List<(string Column, string DataType, bool DifferentMetadata)> DeletedColumns = FindNewColumns(DestinationColumns, SourceColumns);

                Dts.Events.FireInformation(0, "Generation " + DateTime.Now.TimeOfDay, "Building Source_NewTablesColumns...", string.Empty, 0, ref skip);
                List<string> Source_NewTablesColumns = FindTables(NewColumns, Prefix, Datamart, sourceConnection);

                Dts.Events.FireInformation(0, "Generation " + DateTime.Now.TimeOfDay, "Building SourceQueries...", string.Empty, 0, ref skip);
                Dictionary<string, string> SourceQueries = GenerateInstructionsForNewInDestination(NewColumns, Datamart);
                #endregion

                // DROP COLUMN DBO
                string AlterDrop = GenerateAlterDrop(DeletedColumns, Datamart);
                Dts.Events.FireInformation(0, "Alter " + DateTime.Now.TimeOfDay, "Drop dbo:\n" + AlterDrop, string.Empty, 0, ref skip);

                // ADD COLUMN DBO
                string AlterAdd = SourceQueries["AddSQL"];
                Dts.Events.FireInformation(0, "Alter " + DateTime.Now.TimeOfDay, "Add dbo:\n" + AlterAdd, string.Empty, 0, ref skip);

                // ALTER COLUMN DBO
                string AlterColumn = SourceQueries["AlterColumnSQL"];
                Dts.Events.FireInformation(0, "Alter " + DateTime.Now.TimeOfDay, "Column dbo:\n" + AlterColumn, string.Empty, 0, ref skip);

                // TRUNCATE DELTATRANS
                string TruncateDeltaTrans = "TRUNCATE TABLE deltaTrans.[" + Datamart + "]";
                Dts.Events.FireInformation(0, "deltaTrans " + DateTime.Now.TimeOfDay, "Truncate deltaTrans:\n" + TruncateDeltaTrans, string.Empty, 0, ref skip);

                // ADD COLUMN DELTATRANS
                string AlterAddDeltaTrans = SourceQueries["AddDelta"];
                Dts.Events.FireInformation(0, "deltaTrans " + DateTime.Now.TimeOfDay, "Add deltaTrans:\n" + AlterAddDeltaTrans, string.Empty, 0, ref skip);

                // SELECT Source
                string ReadSource = GenerateSourceRead(NewColumns, PKColumnList, Source_NewTablesColumns, Datamart, StartDate, destinationConnection);
                Dts.Events.FireInformation(0, "deltaTrans " + DateTime.Now.TimeOfDay, "Source Extraction:\n" + ReadSource, string.Empty, 0, ref skip);

                // DROP COLUMN DELTATRANS
                string AlterDropDeltaTrans = SourceQueries["DropDelta"];
                Dts.Events.FireInformation(0, "deltaTrans " + DateTime.Now.TimeOfDay, "Drop deltaTrans:\n" + AlterDropDeltaTrans, string.Empty, 0, ref skip);

                // UPDATE DBO INNER DELTA
                string UpdateDestination = GenerateUpdatedbo(NewColumns, PKColumnList, Datamart, StartDate);
                Dts.Events.FireInformation(0, "deltaTrans " + DateTime.Now.TimeOfDay, "Update Destination:\n" + UpdateDestination, string.Empty, 0, ref skip);

                #region SSIS Variable Writing
                Dts.Variables["User::AlterAdd"].Value = AlterAdd;
                Dts.Variables["User::AlterDrop"].Value = AlterDrop;
                Dts.Variables["User::AlterColumn"].Value = AlterColumn;
                Dts.Variables["User::TruncateDeltaTrans"].Value = TruncateDeltaTrans;
                Dts.Variables["User::AddDeltaTrans"].Value = AlterAddDeltaTrans;
                Dts.Variables["User::LecturaNuevosBC"].Value = ReadSource;
                Dts.Variables["User::DropDeltaTrans"].Value = AlterDropDeltaTrans;
                Dts.Variables["User::UpdateHIS"].Value = UpdateDestination;
                #endregion

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception ex)
            {//If the script fails
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, ex.Message, null, 0);
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, ex.StackTrace, string.Empty, 0);
                Dts.TaskResult = (int)ScriptResults.Failure;
            }

            // Close connections
            destinationConnection.Close();
            sourceConnection.Close();
            #endregion
        }
        #region Auxiliary Functions
        private SqlDataReader ExtractData(string Query, SqlConnection Connection)
        {
            SqlCommand command = new SqlCommand(Query, Connection);
            command.CommandTimeout = 180; // 3 minutes
            SqlDataReader sqlData = command.ExecuteReader();
            return sqlData;
        }
        private List<(string Column, string DataType, bool OnlyDataType)> FindNewColumns(DataTable SourceColumns, DataTable DestinationColumns)
        {// Function to find source columns that are not in destination
            List<(string Column, string DataType, bool DifferentDatatype)> Columns = new List<(string, string, bool)>();
            bool sameColumn;
            bool sameDatatype;
            foreach (DataRow SourceRow in SourceColumns.Rows)
            {
                // Strategy: Doesn't exist and is different until proven otherwise
                sameColumn = false;
                sameDatatype = false;
                foreach (DataRow DestinationRow in DestinationColumns.Rows)
                {
                    if (SourceRow[0].Equals(DestinationRow[0]) || SourceRow[0].Equals("timestamp"))
                    {
                        sameColumn = true;
                        if (SourceRow[1].Equals(DestinationRow[1]) || SourceRow[0].Equals("timestamp"))
                        {
                            sameDatatype = true;
                        }
                        break;
                    }
                }

                if (sameColumn == false)
                {// If the column doesn't exist
                    Columns.Add((SourceRow[0].ToString(), SourceRow[1].ToString(), false));
                }

                                if (sameColumn == true && sameDatatype == false)
                {// If it exists with a different datatype
                    Columns.Add((SourceRow[0].ToString(), SourceRow[1].ToString(), true));
                }
            }

            return Columns;
        }
        private List<string> FindTables(List<(string Column, string DataType, bool OnlyDataType)> Columns, string Prefix, string Datamart, SqlConnection DB)
        {// Function to find the tables where the new fields exist
            List<string> Tables = new List<string>();

            #region Analyze if the datamart has suffixes
            SqlDataReader StudyDatamart = ExtractData(@"Select Sufix 
                                                    from [dbo].[TablesAndColumnsBCSource] 
                                                    WHERE Datamart = '" + Datamart + @"'
                                                        and Prefix = '" + Prefix + @"'
                                                        and IsPKField = 0
                                                    group by Sufix"
                                                , DB);
            int rows = 0;
            string Sufix = "DesiredSufix";
            bool NoSuffixes = false;
            while (StudyDatamart.Read())
            {
                rows++;

                if (rows > 1)
                {
                    // If there is more than one row, it is not necessary to continue checking
                    break;
                }

                // Assuming that "Sufix" is a string column
                Sufix = StudyDatamart.GetString(StudyDatamart.GetOrdinal("Sufix"));
            }
            StudyDatamart.Close();
            if (rows == 1 && Sufix == "")
            {
                NoSuffixes = true;
            }
            #endregion

            if (NoSuffixes == false)
            {
                #region Generate SuffixQuery
                string SuffixQuery = @"SELECT Sufix
                FROM [dbo].[TablesAndColumnsBCSource] t1
                WHERE Datamart = '" + Datamart + @"'
                and Prefix = '" + Prefix + @"'
                and IsPKField = 0";

                foreach ((string Column, _, bool OnlyDataType) in Columns)
                {// The new columns
                    if (Column != "$SystemModifiedAt" && OnlyDataType == false)
                    {// $SystemModifiedAt will always be needed and is in all tables #hardcode
                        if (SuffixQuery.EndsWith("IsPKField = 0"))
                        {// If it is the first column
                            SuffixQuery = SuffixQuery + " and Column in ('$SystemModifiedAt', '" + Column + "'";
                        }
                        else
                        {// If we have already written some column
                            SuffixQuery = SuffixQuery + ", '" + Column + "'";
                        }
                    }
                }

                SuffixQuery = SuffixQuery + @")
                Group by Sufix";
                #endregion

                SqlDataReader reader = ExtractData(SuffixQuery, DB);
                while (reader.Read())
                {// Load the output
                    if (reader["Sufix"].ToString().Substring(0, 4) == "$100")
                    {// The suffix of the main tables starts with $100
                        Tables.Insert(0, Prefix + Datamart + reader["Sufix"].ToString());
                    }
                    else
                    {
                        Tables.Add(Prefix + Datamart + reader["Sufix"].ToString());
                    }
                }
            }
            else
            {// It is a datamart without suffixes
                Tables.Add(Prefix + Datamart);
            }

            return Tables;
        }
        private Dictionary<string, string> GenerateInstructionsForNewInDestination(List<(string Column, string DataType, bool OnlyDataType)> Columns, string Datamart)
        {
            string AddSQL = "ALTER TABLE dbo.[" + Datamart + "] ADD";
            string AddDelta = "ALTER TABLE deltaTrans.[" + Datamart + "] ADD";
            string ColumnSQL = "ALTER TABLE dbo.[" + Datamart + "]";
            string DropDelta = "ALTER TABLE deltaTrans.[" + Datamart + "] DROP COLUMN";

            foreach ((string Column, string DataType, bool OnlyDataType) in Columns)
            {
                if (OnlyDataType == true)
                {// The column exists but with a different datatype
                    ColumnSQL = ColumnSQL + "\n  ALTER COLUMN [" + Column + "] " + DataType + ",";
                }
                else
                {// The column is new
                    AddSQL = AddSQL + "\n  [" + Column + "] " + DataType + " NULL,";
                    AddDelta = AddDelta + "\n  [" + Column + "] " + DataType + " NULL,";
                    DropDelta = DropDelta + "\n  [" + Column + "],";
                }
            }

            AddSQL = AddSQL.EndsWith(",") ? AddSQL.Remove(AddSQL.Length - 1) : "";
            AddDelta = AddDelta.EndsWith(",") ? AddDelta.Remove(AddDelta.Length - 1) : "";
            DropDelta = DropDelta.EndsWith(",") ? DropDelta.Remove(DropDelta.Length - 1) : "";
            ColumnSQL = ColumnSQL.EndsWith(",") ? ColumnSQL.Remove(ColumnSQL.Length - 1) : "";

            return new Dictionary<string, string>
            {
                {"AddSQL", AddSQL },
                {"AddDelta", AddDelta },
                {"DropDelta", DropDelta },
                {"AlterColumnSQL", ColumnSQL }
            };
        }
        private String GenerateAlterDrop(List<(string Column, string DataType, bool OnlyDataType)> Columns, string Datamart)
        {
            string DropSQL = "ALTER TABLE dbo.[" + Datamart + "] DROP COLUMN";

            foreach ((string Column, _, bool OnlyDataType) in Columns)
            {
                if (OnlyDataType == false)
                {
                    DropSQL = DropSQL + "\n  [" + Column + "],";
                }
            }

            DropSQL = DropSQL.EndsWith(",") ? DropSQL.Remove(DropSQL.Length - 1) : "";
            return DropSQL;
        }
        private string GenerateSourceRead(List<(string Column, string DataType, bool OnlyDataType)> Columns, List<string> PKColumns, List<string> Tables, string Datamart, string StartDate, SqlConnection DB)
        {// Function to fetch the necessary values of the new columns to DeltaTrans and thus later make an update

            #region Generate Select
            string SourceRead = "Select ";

            foreach (string Column in PKColumns)
            {
                SourceRead = SourceRead + "\n t1.[" + Column + "],";
            }

            foreach ((string Column, string DataType, bool OnlyDataType) in Columns)
            {
                if (OnlyDataType == false)
                {// Only new columns
                    SourceRead = SourceRead + "\n [" + Column + "],";
                }
            }

            SourceRead = SourceRead.EndsWith(",") ? SourceRead.Remove(SourceRead.Length - 1) : SourceRead;
            #endregion

            #region Generate FROM
            for (int i = 0; i < Tables.Count; i++)
            {
                if (i == 0)
                {// The first table is built with FROM, the rest INNER JOIN
                    SourceRead = SourceRead + "\nFROM dbo.[" + Tables[i] + "] t1";
                }
                else
                {
                    SourceRead = SourceRead + "\n  INNER JOIN dbo.[" + Tables[i] + "] t" + (i + 1);
                    foreach (string PK in PKColumns)
                    {
                        if (PK != "timestamp")
                        {
                            if (SourceRead.EndsWith(" t" + (i + 1)))
                            {
                                SourceRead = SourceRead + "\n    ON  t1.[" + PK
                                         + "] = t" + (i + 1) + ".[" + PK + "]";
                            }
                            else
                            {
                                SourceRead = SourceRead + "\n    AND t1.[" + PK
                                         + "] = t" + (i + 1) + ".[" + PK + "]";
                            }
                        }
                    }
                }
            }
            #endregion

            SourceRead = SourceRead + "\nWHERE [$SystemModifiedAt] <= '" + StartDate + "'";

            return SourceRead;
        }
        private string GenerateUpdatedbo(List<(string Column, string DataType, bool OnlyDataType)> Columns, List<string> PKColumns, string Datamart, string StartDate)
        {
            string UpdateSQL = "UPDATE t1\n  SET ";

            foreach ((string Column, _, bool OnlyDataType) in Columns)
            {
                if (OnlyDataType == false)
                {// Only new columns
                    UpdateSQL = UpdateSQL + "\n  t1.[" + Column + "] = t2.[" + Column + "],";
                }
            }

            UpdateSQL = UpdateSQL.EndsWith(",") ? UpdateSQL.Remove(UpdateSQL.Length - 1) : "";

            UpdateSQL = UpdateSQL + "\nFROM dbo.[" + Datamart + "] t1";
            UpdateSQL = UpdateSQL + "\nINNER JOIN deltaTrans.[" + Datamart + "] t2";

            foreach (string Column in PKColumns)
            {
                if (Column != "timestamp")
                {
                    if (UpdateSQL.EndsWith(" t2"))
                    {
                        UpdateSQL = UpdateSQL + "\n    ON  t1.[" + Column
                                 + "] = t2.[" + Column + "]";
                    }
                    else
                    {
                        UpdateSQL = UpdateSQL + "\n    AND t1.[" + Column
                                                                  + "] = t2.[" + Column + "]";
                    }
                }
            }

            UpdateSQL = UpdateSQL + "\nWHERE t1.[$SystemModifiedAt] <= '" + StartDate + "'";

            return UpdateSQL;
        }
        private List<string> ColumnToList(DataTable Table, int column)
        {
            List<string> Output = new List<string>();
            foreach (DataRow Row in Table.Rows)
            {
                Output.Add(Row[column].ToString());
            }
            return Output;
        }
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion
    }
}


