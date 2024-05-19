#region Help:  Introduction to the script task
/* The Script Task allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services control flow. 
 * 
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script task. */
#endregion

#region Namespaces
using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Runtime.InteropServices;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.Collections.Generic;
#endregion

namespace MetadataComparisonSourceToDestination
{
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
	public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
	{
		public void Main()
		{
            #region Initializations
            //Reading SSIS Variables
            string QuerySourceColumns = (string)Dts.Variables["SourceColumns"].Value;
            string QueryDestinationColumns = (string)Dts.Variables["DestinationColumns"].Value;

            string Datamart = (string)Dts.Variables["Datamart"].Value;
            string SourceConnectionPath = (string)Dts.Variables["SourcePath"].Value;
            string DestinationConnectionPath = (string)Dts.Variables["DestinationPath"].Value;

            bool skip = true;

            //Initialization and filling of variables
            DataTable SourceColumns = new DataTable();
            DataTable DestinationColumns = new DataTable();
            
            // Database connections
            SqlConnection source = new SqlConnection(SourceConnectionPath);
            source.Open();
            SqlConnection destination = new SqlConnection(DestinationConnectionPath);
            destination.Open();
            #endregion

            #region Data Extraction
            SourceColumns.Load(ExtractData(QuerySourceColumns, source));
            DestinationColumns.Load(ExtractData(QueryDestinationColumns, destination));
            #endregion

            #region Script Execution
            try
            {
                Dts.Events.FireInformation(0, "Comparing " + DateTime.Now.TimeOfDay, "Datamart: " + Datamart, string.Empty, 0, ref skip);
                Dts.Events.FireInformation(0, "Comparing", "Non-PK Columns Source: " + SourceColumns.Rows.Count, string.Empty, 0, ref skip);
                Dts.Events.FireInformation(0, "Comparing", "Non-PK Columns Destination: " + DestinationColumns.Rows.Count, string.Empty, 0, ref skip);

                //Compare column names
                bool DifferentColumns = TablesDifferent(SourceColumns, DestinationColumns, 0);
                //Compare column names and datatypes
                bool DifferentTypes = TablesDifferent(SourceColumns, DestinationColumns, 1);

                //Bit: columns with different names
                Dts.Events.FireInformation(0, "Comparing", "Different Columns: " + DifferentColumns, string.Empty, 0, ref skip);
                if (DifferentColumns)
                {
                    Dts.Variables["DifferentColumns"].Value = true;
                }
                else
                { Dts.Variables["DifferentColumns"].Value = false; }

                //Bit: columns with different datatype
                Dts.Events.FireInformation(0, "Comparing", "Different datatype: " + DifferentTypes, string.Empty, 0, ref skip);
                if (DifferentTypes)
                {
                    Dts.Variables["DifferentTypes"].Value = true;
                }
                else
                { Dts.Variables["DifferentTypes"].Value = false; }

                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception ex)
            {
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, "Message: " + ex.Message, null, 0);
                Dts.Events.FireError(0, "Script Task " + DateTime.Now.TimeOfDay, "StackTrace: " + ex.StackTrace, null, 0);
                Dts.TaskResult = (int)ScriptResults.Failure;
            }
            #endregion

            #region Close connections
            source.Close();
            destination.Close();
            #endregion
        }

        #region Auxiliar Functions
        private SqlDataReader ExtractData(string Query, SqlConnection Connection)
        {//Execute a query on a connection and return a SqlDataReader object with the data
            SqlCommand command = new SqlCommand(Query, Connection);
            command.CommandTimeout = 180; // 3 minutes
            SqlDataReader sqlData = command.ExecuteReader();
            return sqlData;
        }
        public bool TablesDifferent(DataTable Source, DataTable Destination, int CompareDatatype)
        {
            bool columnsEqual;
            bool datatypesEqual;
            foreach (DataRow SourceRow in Source.Rows)
            {
                columnsEqual = false;
                datatypesEqual = false;
                foreach(DataRow DestinationRow in Destination.Rows)
                {
                    if (SourceRow[0].Equals(DestinationRow[0])|| SourceRow[0].Equals("timestamp"))
                    {
                        columnsEqual = true;
                        if (SourceRow[1].Equals(DestinationRow[1]) || SourceRow[0].Equals("timestamp"))
                        {
                            datatypesEqual = true;
                        }
                        break;
                    }              
                }
                if(CompareDatatype == 0)
                {
                    if (columnsEqual == false)
                    {//Source column not found in destination
                        return true;
                    }
                }

                if (CompareDatatype == 1)
                {
                    if (columnsEqual == true && datatypesEqual == false)
                    {//Source column exists with different metadata in destination
                        return true;
                    }
                }
            }

            return false;
        }
        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion
    }
}
