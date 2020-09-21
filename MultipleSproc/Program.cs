using System;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Linq;
using System.Data;
using System.Text;
using System.Configuration;
using System.Security.Cryptography;
using System.Collections.Generic;
using System.Diagnostics;


namespace MultipleSproc
{
    class Program
    {

        static DataTable QueryOps(SqlConnection Conn, DataRow Row, string Sproc, string[] Param)
        {
            /*if sproc is procTopLine, then fill in the table structure */
            var SqlCount = new DataTable();
            if (Sproc == "procTopLine")

            {

                SqlCount.Columns.Add("CompanyId");
                SqlCount.Columns.Add("CompanyName");
                SqlCount.Columns.Add("DocumentCount");
                SqlCount.Columns.Add("TotalSalesAmount", typeof(int));
                SqlCount.Columns.Add("Discount", typeof(int));
                SqlCount.Columns.Add("TotalExempt", typeof(int));
                SqlCount.Columns.Add("TaxableSales", typeof(int));
                SqlCount.Columns.Add("TotalSalesTaxAmount", typeof(int));


                try
                {


                    /*Call sproc with values from each row in parameters data table*/

                    SqlCommand Proc = new SqlCommand("dbo." + Sproc, Conn);
                    Proc.CommandType = CommandType.StoredProcedure;

                    for (int i = 0; i < Row.Table.Columns.Count - 1; i++)
                    {
                        Console.WriteLine("@" + Param[i] + Row[i]);
                        Proc.Parameters.AddWithValue("@" + Param[i], Row[i]);
                    }


                    var getTimer = Stopwatch.StartNew();

                    /*Export Sql Data to DataTable*/

                    using (var GetResults = new SqlDataAdapter(Proc))

                    {
                        GetResults.Fill(SqlCount);


                    }

                    getTimer.Stop();

                    double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds/1000.00, 2);

                    foreach (DataColumn col in SqlCount.Columns)
                    {
                        if (col.ColumnName.Contains("CompanyName"))
                        {
                            for (int i = 0; i < SqlCount.Rows.Count; i++)
                            {
                                if ((SqlCount.Rows[i]["CompanyName"].ToString().Contains("'")))
                                {
                                    SqlCount.Rows[i]["CompanyName"] = SqlCount.Rows[i]["CompanyName"].ToString().Replace("'", "\\'");

                                }

                            }
                        }
                    }

                    SqlCount.Columns.Add(new DataColumn("SqlTime"));

                    for (int i = 0; i < SqlCount.Rows.Count; i++)
                    {

                        SqlCount.Rows[i]["SqlTime"] = ElapseTime;
                        
                    }

                    return SqlCount;
                }
                catch (Exception Ex)
                {
                    Console.WriteLine(Ex.Message.ToString());
                    return null;
                }



            }

            else
            {

                try
                {
                    var getTimer = Stopwatch.StartNew();

                    /*Call sproc with values from each row in parameters data table*/

                    SqlCommand Proc = new SqlCommand("dbo." + Sproc, Conn);
                    Proc.CommandType = CommandType.StoredProcedure;

                    for (int i = 0; i < Row.Table.Columns.Count - 1 ; i++)
                    {
                        Console.WriteLine("@" + Param[i] + Row[i]);
                        Proc.Parameters.AddWithValue("@" + Param[i], Row[i]);
                    }

                    getTimer.Stop();

                    double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds / 1000.00, 2);

                    /*Export Sql Data to DataTable*/

                    using (var GetResults = new SqlDataAdapter(Proc))

                    {
                        GetResults.Fill(SqlCount);


                    }

                    foreach (DataColumn col in SqlCount.Columns)
                    {
                        if (col.ColumnName.Contains("CompanyName"))
                        {
                            for (int i = 0; i < SqlCount.Rows.Count; i++)
                            {
                                if ((SqlCount.Rows[i]["CompanyName"].ToString().Contains("'")))
                                {
                                    SqlCount.Rows[i]["CompanyName"] = SqlCount.Rows[i]["CompanyName"].ToString().Replace("'", "\\'");

                                }

                            }
                        }
                    }

                    /*Account for case sesnsitivity and date formats that can affect hash mismatches*/
                    List<string> hashes = new List<string>();
                    List<string> values = new List<string>();



                    foreach (DataRow R in SqlCount.Rows)
                    {
                        List<string> lists = new List<string>();

                        foreach (DataColumn Col in R.Table.Columns)
                        {

                            /*      if (Col.ColumnName.Contains("Date") || Col.ColumnName.Contains("DATE"))
                                  {

                                      Console.WriteLine(Data + " " + Data.GetType());
                                      string[] Dates = Data.ToString().Split(new char[0]);
                                      string Date = Dates[0];

                                      lists.Add(Date);


                                  } */

                            string Pattern = @"^[0-9]+\.+[0-9]*";
                            string Data = R[Col].ToString();
                            Match w = Regex.Match(Data, Pattern);

                            if (w.Success)
                            {

                                lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                            }

                            else if (Col.ColumnName.ToString().ToLower().Contains("total") || Col.ColumnName.ToString().ToLower().Contains("amount"))
                            {
                                lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                                //Console.WriteLine(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                            }

                            else if (Data.ToLower() == "null")
                            {

                                lists.Add("");

                            }

                            else
                            {


                                lists.Add(R[Col].ToString().ToLower());
                            }

                        }

                        var rec = string.Join(",", lists.ToArray());
                        //Console.WriteLine(rec);
                        values.Add(rec);
                        
                    }

                    Console.WriteLine(values[0]);

                    using (SHA1Managed sha1 = new SHA1Managed())
                    {

                        foreach (var value in values)
                        {

                            var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(value));
                            var sb = new StringBuilder(hash.Length * 2);



                            foreach (byte b in hash)
                            {
                                // convert to hexidecimal
                                sb.Append(b.ToString("X2"));
                            }


                            hashes.Add(sb.ToString());
                            //Console.WriteLine(value + " " + sb.ToString());
                        }
                    }


                    SqlCount.Columns.Add(new DataColumn("hash"));
                    SqlCount.Columns.Add(new DataColumn("SqlTime"));

                    for (int i = 0; i < SqlCount.Rows.Count; i++)
                    {

                        SqlCount.Rows[i]["hash"] = hashes[i];
                        //Console.WriteLine("sql" + " " + hashes[i]);
                        SqlCount.Rows[i]["SqlTime"] = ElapseTime;
                    }

                    return SqlCount;
                }

                catch (SqlException Ex)
                {
                    Console.WriteLine(Ex.Message.ToString());
                    return null;
                }

            }
        }
        static DataTable SnowOps(OdbcConnection Connect, DataRow Row, string Sproc, string[] variables, string Query)
        {

            var SnowCount = new DataTable();
            /*Call sproc with values from each row in parameters data table*/

            string Call = "(";

            if (Sproc == "procTopLine")

            {

                SnowCount.Columns.Add("CompanyId");
                SnowCount.Columns.Add("CompanyName");
                SnowCount.Columns.Add("DocumentCount");
                SnowCount.Columns.Add("TotalSalesAmount", typeof(int));
                SnowCount.Columns.Add("Discount", typeof(int));
                SnowCount.Columns.Add("TotalExempt", typeof(int));
                SnowCount.Columns.Add("TaxableSales", typeof(int));
                SnowCount.Columns.Add("TotalSalesTaxAmount", typeof(int));





                for (int i = 0; i < variables.Length; i++)
                {
                    Call += "?,";
                }


                Call += "?)";

                try
                {

                    /*Call Snowflake sproc with values from each row in parameters data table*/

                    OdbcCommand GetHash = new OdbcCommand("call " + Sproc + Call, Connect);
                    GetHash.CommandType = CommandType.StoredProcedure;
                    for (int i = 0; i < variables.Length; i++)
                    {

                        GetHash.Parameters.AddWithValue("@" + variables[i], Row[i]);

                    }



                    GetHash.Parameters.AddWithValue("@ReportUI_uuid", DBNull.Value);

                    /*Get the hash value*/

                    var getTimer = Stopwatch.StartNew();

                    string Hash = (string)GetHash.ExecuteScalar();

                    getTimer.Stop();

                    double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds/1000.00,2);

                    


                    /*Select statement with hash value to retrieve result set*/

                    OdbcCommand GetDocs = new OdbcCommand(Query + " where UUID = ? ", Connect);
                    GetDocs.Parameters.AddWithValue("@Hash", Hash);

                    /*Export result set to data table*/

                    
                    using (var DocsResults = new OdbcDataAdapter(GetDocs))

                    {
                        DocsResults.Fill(SnowCount);


                    }


                    foreach (DataColumn col in SnowCount.Columns)
                    {
                        if (col.ColumnName.Contains("CompanyName"))
                        {
                            for (int i = 0; i < SnowCount.Rows.Count; i++)
                            {
                                if ((SnowCount.Rows[i]["CompanyName"].ToString().Contains("'")))
                                {
                                    SnowCount.Rows[i]["CompanyName"] = SnowCount.Rows[i]["CompanyName"].ToString().Replace("'", "\\'");

                                }

                            }
                        }
                    }

                    SnowCount.Columns.Add(new DataColumn("SnowTime"));

                    for (int i = 0; i < SnowCount.Rows.Count; i++)
                    {

                        SnowCount.Rows[i]["SnowTime"] = ElapseTime;
                        

                    }

                    return SnowCount;

                }

                catch (Exception Ex)
                {
                    Console.WriteLine(Ex.Message.ToString());
                    return null;
                }



            }

            else
            {

                for (int i = 0; i < variables.Length; i++)
                {
                    Call += "?,";
                }


                Call += "?)";


                try
                {

                    /*Call Snowflake sproc with values from each row in parameters data table*/

                    OdbcCommand GetHash = new OdbcCommand("call " + Sproc + Call, Connect);
                    GetHash.CommandType = CommandType.StoredProcedure;
                    for (int i = 0; i < variables.Length; i++)
                    {

                        GetHash.Parameters.AddWithValue("@" + variables[i], Row[i]);

                    }



                    GetHash.Parameters.AddWithValue("@ReportUI_uuid", DBNull.Value);

                    /*Get the hash value*/

                    var getTimer = Stopwatch.StartNew();

                    string Hash = (string)GetHash.ExecuteScalar();

                    getTimer.Stop();

                    double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds/1000.00, 2);


                    /*Select statement with hash value to retrieve result set*/

                    OdbcCommand GetDocs = new OdbcCommand(Query + " where UUID = ? ", Connect);
                    GetDocs.Parameters.AddWithValue("@Hash", Hash);

                    /*Export result set to data table*/

                    using (var DocsResults = new OdbcDataAdapter(GetDocs))

                    {
                        DocsResults.Fill(SnowCount);


                    }


                    /*Account for case sesnsitivity and date formats that can affect hash mismatches*/
                    List<string> hashes = new List<string>();
                    List<string> values = new List<string>();
                   
                    foreach (DataColumn col in SnowCount.Columns)
                    {
                        if (col.ColumnName.Contains("CompanyName"))
                        {
                            for (int i = 0; i < SnowCount.Rows.Count; i++)
                            {
                                if ((SnowCount.Rows[i]["CompanyName"].ToString().Contains("'")))
                                {
                                    SnowCount.Rows[i]["CompanyName"] = SnowCount.Rows[i]["CompanyName"].ToString().Replace("'", "\\'");

                                }

                            }
                        }
                    }


                    foreach (DataRow R in SnowCount.Rows)
                    {
                        List<string> lists = new List<string>();

                        foreach (DataColumn Col in R.Table.Columns)
                        {

                            /*   if (Col.ColumnName.Contains("Date") || Col.ColumnName.Contains("DATE"))
                               {

                                   Console.WriteLine(Data + " " + Data.GetType());
                                   string[] Dates = Data.ToString().Split(new char[0]);
                                   string Date = Dates[0];

                                   lists.Add(Date);


                               }*/

                            string Pattern = @"^[0-9]+\.+[0-9]*";
                            string Data = R[Col].ToString();
                            Match w = Regex.Match(Data, Pattern);

                            if (w.Success)
                            {

                                lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                            }

                            else if (Col.ColumnName.Contains("TOTAL") || Col.ColumnName.Contains("AMOUNT"))
                            {
                                lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());

                            }

                            else if (Data.ToLower() == "null")
                            {

                                lists.Add("");

                            }

                            else
                            {

                                lists.Add(R[Col].ToString().ToLower());
                            }

                        }

                        var rec = string.Join(",", lists.ToArray());
                        //Console.WriteLine(rec);
                        values.Add(rec);
                        
                    }

                    Console.WriteLine(values[0]);

                    using (SHA1Managed sha1 = new SHA1Managed())
                    {
                        foreach (var value in values)
                        {

                            var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(value));
                            var sb = new StringBuilder(hash.Length * 2);



                            foreach (byte b in hash)
                            {
                                // convert to hexidecimal
                                sb.Append(b.ToString("X2"));
                            }


                            hashes.Add(sb.ToString());
                           // Console.WriteLine(value + " " + sb.ToString());
                        }
                    }

                    SnowCount.Columns.Add(new DataColumn("hash"));
                    SnowCount.Columns.Add(new DataColumn("SnowTime"));

                    for (int i = 0; i < SnowCount.Rows.Count; i++)
                    {

                        SnowCount.Rows[i]["hash"] = hashes[i];
                        //Console.WriteLine("Snow " + " " + hashes[i]);
                        SnowCount.Rows[i]["SnowTime"] = ElapseTime;

                    }



                    return SnowCount;
                }

                catch (OdbcException Ex)
                {

                    Console.WriteLine(Ex.Message.ToString());

                    return null;
                }
            }
        }
        static void MatchOnly(DataTable dt, DataTable dt1, string Company, string StartDate, string EndDate, string ProcName, OdbcConnection Conn, int TableId)

        {

            string TestResult;
            string TestName = Company + "_" + StartDate + "_" + EndDate;
            string Today = DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss");
            string RowName = "RowCount";
            double SnowTime = Convert.ToDouble(dt1.Rows[0]["SnowTime"]);
            double SqlTime = Convert.ToDouble(dt.Rows[0]["SqlTime"]);

            if (dt.Rows.Count == dt1.Rows.Count)
            {

                TestResult = "Pass";
            }

            else { TestResult = "Fail"; }

            Console.WriteLine(TestResult);

            string OdbcComm = "Insert into UnitTests Values('" + TestName + "','" + RowName + "','" + dt1.Rows.Count + "','" + dt.Rows.Count + "','" + TestResult + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";

            OdbcCommand Ins = new OdbcCommand(OdbcComm, Conn);

            Ins.ExecuteNonQuery();



            /*  var Inner = from Snow in dt1.AsEnumerable()
                          join
                    Sql in dt.AsEnumerable() on
                    Snow.Field<string>("hash") equals Sql.Field<string>("hash")
                          select Snow; */

            /*Snowflake only datarows*/

            var OnlySnow = from Snow in dt1.AsEnumerable()
                           join
                     Sql in dt.AsEnumerable() on
                     Snow.Field<string>("hash") equals Sql.Field<string>("hash")
                     into x
                           from Sql in x.DefaultIfEmpty()
                           where Sql == null
                           select Snow;



            foreach (var Snow in OnlySnow)
            {

                List<string> lists = new List<string>();

                foreach (DataColumn col in Snow.Table.Columns)
                {


                    if (col.ColumnName.Contains("DATE"))
                    {
                        string Data = Snow[col].ToString();
                        Console.WriteLine(Data + " " + Data.GetType());
                        string[] Dates = Data.ToString().Split(new char[0]);
                        string Date = Dates[0];
                        Console.WriteLine(Date);
                        lists.Add(Date);


                    }
                    else
                    {
                        lists.Add(Snow[col].ToString());
                    }
                }

                string Hash = Snow["hash"].ToString();

                lists.RemoveAt(lists.Count - 1);
                var rec = string.Join("','", lists);
                Console.WriteLine(rec);
                string InsertHash = "Insert into UnitTests Values('" + TestName + "','" + "RowHash" + "','" + Hash + "','" + " " + "','" + "Fail" + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime +  "')";

                OdbcCommand InsertH = new OdbcCommand(InsertHash, Conn);

                InsertH.ExecuteNonQuery();


                string Insert = "Insert into " + ProcName + " Values('" + rec + "','" + Today + "','" + TestName + "')";
                Console.WriteLine(Insert);
                OdbcCommand InsertData = new OdbcCommand(Insert, Conn);

                InsertData.ExecuteNonQuery();

            }

            /*Sql only datarows*/

            var OnlySql = from Sql in dt.AsEnumerable()
                          join
                            Snow in dt1.AsEnumerable()
                      on
                     Sql.Field<string>("hash") equals Snow.Field<string>("hash")
                     into x
                          from Snow in x.DefaultIfEmpty()
                          where Snow == null
                          select Sql;


            foreach (var Sql in OnlySql)
            {

                List<string> lists = new List<string>();


                foreach (DataColumn col in Sql.Table.Columns)
                {
                    if (col.ColumnName.Contains("Date") || col.ColumnName.Contains("DATE"))
                    {
                        string Data = Sql[col].ToString();
                        Console.WriteLine(Data + " " + Data.GetType());
                        string[] Dates = Data.ToString().Split(new char[0]);
                        string Date = Dates[0];


                        lists.Add(Date);

                    }

                    else
                    {
                        lists.Add(Sql[col].ToString());
                    }
                }
                string Hash = Sql["hash"].ToString();
                lists.RemoveAt(lists.Count - 1);
                var rec = string.Join("','", lists);
                Console.WriteLine(rec);
                string InsertHash = "Insert into UnitTests Values('" + TestName + "','" + "RowHash" + "','" + " " + "','" + Hash + "','" + "Fail" + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";

                OdbcCommand InsertH = new OdbcCommand(InsertHash, Conn);

                InsertH.ExecuteNonQuery();

                string Insert = "Insert into " + ProcName + " Values('" + rec + "','" + Today + "','" + TestName + "')";

                OdbcCommand InsertData = new OdbcCommand(Insert, Conn);

                InsertData.ExecuteNonQuery();
            }
        }

        static void MatchAggOnly(DataTable dt, DataTable dt1, string Company, string StartDate, string EndDate, string ProcName, OdbcConnection Conn, int TableId)
        {
            string TestResult;
            string TestName = Company + "_" + StartDate + "_" + EndDate;
            string Today = DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss");
            double SnowTime = Convert.ToDouble(dt1.Rows[0]["SnowTime"]);
            double SqlTime = Convert.ToDouble(dt.Rows[0]["SqlTime"]);

            List<string> Metrics = new List<string>();

            Metrics.Add("CompanyId");
            Metrics.Add("CompanyName");
            Metrics.Add("DocumentCount");
            Metrics.Add("TotalSalesAmount");
            Metrics.Add("Discount");
            Metrics.Add("TaxableSales");
            Metrics.Add("TotalExempt");
            Metrics.Add("TotalSalesTaxAmount");

            if (dt.Rows.Count == dt1.Rows.Count)
            {

                TestResult = "Pass";
            }

            else { TestResult = "Fail"; }

            string Command = "Insert into UnitTests Values('" + TestName + "','" + "RowCount" + "','" + dt1.Rows.Count + "','" + dt.Rows.Count + "','" + TestResult + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";

            OdbcCommand Ins = new OdbcCommand(Command, Conn);

            Ins.ExecuteNonQuery();

            for (int i = 0; i < dt.Rows.Count; i++)
            {

                foreach (string Metric in Metrics)
                {
                    if (dt1.Rows[i][Metric].ToString().ToLower() == dt.Rows[i][Metric].ToString().ToLower())
                    {
                        TestResult = "Pass";
                        string Comm = "Insert into UnitTests Values('" + TestName + "','" + Metric + "','" + dt1.Rows[i][Metric].ToString() + "','" + dt.Rows[i][Metric].ToString() + "','" + TestResult + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";
                        Console.WriteLine(Comm);

                        OdbcCommand Query = new OdbcCommand(Comm, Conn);
                        Query.ExecuteNonQuery();
                    }

                    else
                    {
                        TestResult = "Fail";

                        string Comm = "Insert into UnitTests Values('" + TestName + "','" + Metric + "','" + dt1.Rows[i][Metric].ToString() + "','" + dt.Rows[i][Metric].ToString() + "','" + TestResult + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";
                        Console.WriteLine(Comm);

                        OdbcCommand Query = new OdbcCommand(Comm, Conn);
                        Query.ExecuteNonQuery();
                    }
                }
            }
        }
        static void Main(string[] args)
        {

            string SqlConn = ConfigurationManager.ConnectionStrings["SqlProdConn"].ConnectionString;
            string Connect = ConfigurationManager.ConnectionStrings["ProdConnection"].ConnectionString;
            string Target = ConfigurationManager.ConnectionStrings["ProdConnectionTarget"].ConnectionString;




            using (SqlConnection Conn = new SqlConnection(SqlConn))
            {
                Conn.Open();

                using (OdbcConnection SnowConnection = new OdbcConnection(Connect))
                {
                    SnowConnection.Open();

                    using (OdbcConnection TConnection = new OdbcConnection(Target))
                    {

                        TConnection.Open();
                        List<string> ProcNames = new List<string>();



                        try
                        {
                            /*iterate thru procnames*/


                            //ProcNames.Add("procTopLine");
                            //ProcNames.Add("procDocumentSummaryReport");
                            // ProcNames.Add("procRptSalesUseTaxJurisDetail_Recap");

                            // ProcNames.Add("procRptDocumentSummaryByListing");
                            //ProcNames.Add("procRptTaxByJurisReport");
                            //  ProcNames.Add("procRptTaxJurisDetailView_CombinedViewTaxingJuris");
                            //ProcNames.Add("procRptReconcileDocLineDetail");

                            OdbcCommand GetProcs = new OdbcCommand("Select distinct ProcName from nUnitTesting where IsExport = 'FALSE' and ProcName = 'procRptSalesUseTaxJurisdiction'", SnowConnection);
                            OdbcDataReader ResultVal = GetProcs.ExecuteReader();

                            while (ResultVal.Read())
                            {
                                ProcNames.Add(ResultVal["ProcName"].ToString());

                            }

                            foreach (string ProcName in ProcNames)
                            {
                                Console.WriteLine(ProcName);

                                DataTable Pdt = new DataTable();
                                int x, y, z;
                                StringBuilder SqlQuery = new StringBuilder("Select ");


                                /*Get Parameter Names from control table*/


                                string Query;


                                OdbcCommand GetValuesNames = new OdbcCommand("Select Parameters from nUnitTesting where ProcName = '" + ProcName + "'", SnowConnection);

                                string Para = (string)GetValuesNames.ExecuteScalar();



                                string[] Parameters = Para.Split(",");



                                x = ((Array.IndexOf(Parameters, "CompanyId") == -1) || String.IsNullOrEmpty(Parameters[Array.IndexOf(Parameters, "CompanyId")])) ? Array.IndexOf(Parameters, "ParentId") : Array.IndexOf(Parameters, "CompanyId");
                                Console.WriteLine(x);
                                y = Array.IndexOf(Parameters, "StartDate");
                                z = Array.IndexOf(Parameters, "EndDate");
                                Console.WriteLine(x + "," + y + "," + z);

                                for (int NumParams = 1; NumParams < Parameters.Length; NumParams++)
                                {


                                    SqlQuery.Append("Para" + NumParams + ", ");

                                }


                                /*With Parameters.Length, we can get the actual values for each of the parameters from the control table*/

                                SqlQuery.Append("Para" + Parameters.Length + " ,TableId" + " from nUnitTesting where ProcName = '" + ProcName + "'");

                                Console.WriteLine(SqlQuery.ToString());

                                OdbcCommand GetParameterValues = new OdbcCommand(SqlQuery.ToString(), SnowConnection);

                                /* Store all permutations in our parameters datatable*/

                                using (var Y = new OdbcDataAdapter(GetParameterValues))
                                {
                                    Y.Fill(Pdt);

                                }

                                
                        
                                /* For each permutation in our permuations datatable, make a call to snowflake & sql server to get result sets*/

                                OdbcCommand GetSnowTable = new OdbcCommand("Select SnowflakeTable from nUnitTesting where ProcName = '" + ProcName + "'", SnowConnection);

                                string SnowflakeTable = GetSnowTable.ExecuteScalar().ToString();

                                Console.WriteLine(SnowflakeTable);

                                foreach (DataRow dr in Pdt.Rows)
                                {
                                    List<string> DC = new List<string>();

                                    DataTable Tab;
                                    DataTable SnowTab;
                                    var TableId = Convert.ToInt32(dr["TableId"].ToString());


                                    Tab = QueryOps(Conn, dr, ProcName, Parameters);


                                    if (ProcName != "procTopLine")
                                    {
                                        for (int i = 0; i < Tab.Columns.Count - 2; i++)
                                        {
                                            DC.Add(Tab.Columns[i].ColumnName);

                                        }

                                        Query = "Select " + string.Join(",", DC) + " from " + SnowflakeTable;

                                        Console.WriteLine(Query);
                                    }

                                    else
                                    {
                                        for (int i = 0; i < Tab.Columns.Count - 1; i++)
                                        {
                                            DC.Add(Tab.Columns[i].ColumnName);

                                        }

                                        Query = "Select " + string.Join(",", DC) + " from " + SnowflakeTable;

                                        Console.WriteLine(Query);

                                    }



                                    SnowTab = SnowOps(SnowConnection, dr, ProcName, Parameters, Query);

                                    string CompanyId = dr[x].ToString();
                                    string StartDate = dr[y].ToString();
                                    string EndDate = dr[z].ToString();

                                    /*Get Column DataType
                                    foreach (DataColumn Col in Tab.Columns)
                                        {

                                              Console.WriteLine(Tab.Rows[0][Col].GetType());

                                        } */
                                   
                                   // Console.WriteLine(Tab.Rows.Count);

                                   // Console.WriteLine(SnowTab.Rows.Count);



                                    if (ProcName == "procTopLine")
                                    {

                                       MatchAggOnly(Tab, SnowTab, CompanyId, StartDate, EndDate, ProcName, TConnection, TableId);

                                    }

                                    else
                                    {
                                       // MatchOnly(Tab, SnowTab, CompanyId, StartDate, EndDate, ProcName, TConnection, TableId);
                                    }
                                }


                            }
                        }
                        catch (SqlException Ex)
                        {

                            Console.WriteLine(Ex.Message.ToString());

                        }
                    }
                }
            }
        }
    }
}