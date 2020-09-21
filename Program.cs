using System;
using System.Configuration;
using Amazon.S3;
using System.Data.SqlClient;
using Amazon.S3.Model;
using System.IO;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Data.Odbc;
using Amazon.Runtime.CredentialManagement;
using System.Threading;
using System.Diagnostics;

namespace ProcExportDocumentLineDetail
{
    class Program
    {
        static DataTable QueryOps(SqlConnection Conn, DataRow Row, string Sproc, string[] Param)
        {
            var SqlCount = new DataTable();

            try
            {

                /*Call sproc with values from each row in parameters data table*/

                SqlCommand Proc = new SqlCommand("procExportDocumentLineDetail", Conn);
                Proc.CommandType = CommandType.StoredProcedure;
                /* Proc.Parameters.AddWithValue("@AccountId", Row[0]);
                 Proc.Parameters.AddWithValue("@CompanyId", Row[1]);
                 Proc.Parameters.AddWithValue("@StartDate", Row[2]);
                 Proc.Parameters.AddWithValue("@EndDate", Row[3]);
                 Proc.Parameters.AddWithValue("@StartCode", Row[4]);
                 Proc.Parameters.AddWithValue("@EndCode", Row[5]);
                 Proc.Parameters.AddWithValue("@Country", Row[6]);
                 Proc.Parameters.AddWithValue("@State", Row[7]);
                 Proc.Parameters.AddWithValue("@DocumentStatusId", Row[8]);
                 Proc.Parameters.AddWithValue("@DocType", Row[9]);
                 Proc.Parameters.AddWithValue("@CountryId", Row[10]);
                 Proc.Parameters.AddWithValue("@DateFilter", Row[11]);
                 Proc.Parameters.AddWithValue("@CurrencyCode", Row[12]);
                 Proc.Parameters.AddWithValue("@ParentId", Row[13]); */

                for (int i = 0; i < Row.Table.Columns.Count - 2; i++)
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

                double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds / 1000.00, 2);

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

                var hashes = new List<string>();
                
                var values = new List<string>();

                /*Transform Data to align with Snowflake*/

                foreach (DataRow R in SqlCount.Rows)
                {
                    var lists = new List<string>();

                    foreach (DataColumn col in SqlCount.Columns)
                    {
                        string Pattern = @"^[0-9]+\.[0-9]*";
                        string Data = R[col].ToString();
                        Match w = Regex.Match(Data, Pattern);

                        if (w.Success)
                        {

                            lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                        }

                        else if (col.ColumnName.Contains("Date"))
                            {

                                string[] Dates = Data.ToString().Split(new char[0]);


                                lists.Add(Dates[0]);

                            }

                            else if (Data.ToLower() == "null")
                            {

                                lists.Add("");

                            }

                            else
                            {

                                lists.Add(Data.ToString().ToLower());

                            }
                        
                    }
             
                    var rec = string.Join(",", lists.ToArray());
                    
                    values.Add(rec);
                
                 }

                Console.WriteLine(values[0]);

                /*Hash DataRows*/

                foreach (var value in values)
                {
                    using (SHA1Managed sha1 = new SHA1Managed())
                    {

                        var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(value));

                        var sb = new StringBuilder(hash.Length * 2);

                        foreach (byte b in hash)
                        {
                            // can be "x2" if you want lowercase
                            sb.Append(b.ToString("X2"));

                        }

                        hashes.Add(sb.ToString());
                        //Console.WriteLine(sb.ToString());
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
                Console.WriteLine("Sql Server Error - {0}", Ex.Message.ToString());
                Console.ReadLine();
                return null;
            }
            catch (Exception Ex)
            {
                Console.WriteLine("Sql Server Error - {0}", Ex.Message.ToString());
                Console.ReadLine();
                return null;
            }
        }
        static string GetHash(OdbcConnection Connect, DataRow Row, string Sproc, string[] variables, string Stage)
        {
            try
            {
                
                /*Call sproc with values from each row in parameters data table*/

                string Call = "(";

                /*Call Snowflake sproc with values from each row in parameters data table*/



                /*OdbcCommand GetHash = new OdbcCommand("call procExportDocumentLineDetail(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Conn);
                GetHash.CommandType = CommandType.StoredProcedure;
                GetHash.Parameters.AddWithValue("@AccountId", Row[0]);
                GetHash.Parameters.AddWithValue("@CompanyId", Row[1]);
                GetHash.Parameters.AddWithValue("@StartDate", Row[2]);
                GetHash.Parameters.AddWithValue("@EndDate", Row[3]);
                GetHash.Parameters.AddWithValue("@StartCode", Row[4]);
                GetHash.Parameters.AddWithValue("@EndCode", Row[5]);
                GetHash.Parameters.AddWithValue("@Country", Row[6]);
                GetHash.Parameters.AddWithValue("@State", Row[7]);
                GetHash.Parameters.AddWithValue("@DocumentStatusId", Row[8]);
                GetHash.Parameters.AddWithValue("@DocType", Row[9]);
                GetHash.Parameters.AddWithValue("@CountryId", Row[10]);
                GetHash.Parameters.AddWithValue("@DateFilter", Row[11]);
                GetHash.Parameters.AddWithValue("@CurrencyCode", Row[12]);
                GetHash.Parameters.AddWithValue("@ParentId", Row[13]);
                GetHash.Parameters.AddWithValue("@StageName", DBNull.Value);

               

                string Hash = (string)GetHash.ExecuteScalar();

                Console.WriteLine(Hash);

                return Hash; */

                for (int i = 0; i < variables.Length + 1; i++)
                {
                    Call += "?,";
                }


                Call += "?)";



                    /*Call Snowflake sproc with values from each row in parameters data table*/

                    OdbcCommand GetHash = new OdbcCommand("call " + Sproc + Call, Connect);
                    GetHash.CommandType = CommandType.StoredProcedure;
                    for (int i = 0; i < variables.Length; i++)
                    {

                        GetHash.Parameters.AddWithValue("@" + variables[i], Row[i]);

                    }



                    GetHash.Parameters.AddWithValue("@StageName", Stage);
                    GetHash.Parameters.AddWithValue("@FileName", DBNull.Value);

                /*Get the hash value*/

                var getTimer = Stopwatch.StartNew();

                    string Hash = (string)GetHash.ExecuteScalar();

                    getTimer.Stop();

                    double ElapseTime = Math.Round(getTimer.ElapsedMilliseconds / 1000.00, 2);

                    return Hash + "-" + ElapseTime;
                }

            catch (OdbcException Ex)
            {
                Console.WriteLine("Odbc Error - {0}", Ex.Message.ToString());
                Console.ReadLine();
                return null;
            }

        }


        /*Get File from S3 bucket*/
        static void DownLoadFile(string path, AmazonS3Client Client, string Code)

        {
            try
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = "sf-returns-fqa-cqa-data",
                    Key = "EDLD/" + Code + ".csv"
                };

                using (GetObjectResponse response = Client.GetObject(request))
                using (Stream responseStream = response.ResponseStream)
                using (StreamReader r = new StreamReader(responseStream))
                {
                    using (var writer = new StreamWriter(path, append: false))
                        writer.Write(r.ReadToEnd());
                }

                

            }

            catch (Exception Ex)

            {
                Console.WriteLine(Ex.Message.ToString());
                Console.ReadLine();

            }

        }

        /*Extract File*/

        static DataTable ExtractFile(string file)
        {


            /*Read thru each file and load them into DataTable*/

              try

            {

                using (var Re = new StreamReader(file))

                {

                    
                    DataTable Dt = new DataTable();
                    
                    string[] Headers = Regex.Split(Re.ReadLine(), ",");
                    
                    
                    
                    {
                        foreach (string Header in Headers)
                        {

                               
                            Dt.Columns.Add(Header);

                        }
                    }
                    while (!Re.EndOfStream)
                    {
                        var Rows = Regex.Split(Re.ReadLine(), ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");



                        if (Rows.Length == Dt.Columns.Count)
                        {
                            DataRow Dr = Dt.NewRow();
                            
                            Dr.ItemArray = Rows;

                            Dt.Rows.Add(Dr);
                        }



                    }


                    Console.WriteLine(String.Join(" : ", Dt.Rows[0].ItemArray));

                    foreach (DataRow row in Dt.Rows)
                    {
                        foreach (DataColumn col in Dt.Columns)
                        {


                            if (row[col].ToString().Contains("\""))
                            {
                                row[col] = row[col].ToString().Substring(1, row[col].ToString().Length - 2);
                            }
                        }

                    }


                    List<string> hashes = new List<string>();
                    List<string> values = new List<string>();

                    /*Transform Data*/

                    foreach (DataRow R in Dt.Rows)
                    {
                        List<string> lists = new List<string>();
                        foreach (DataColumn col in Dt.Columns)
                        {

                            string Pattern = @"^[0-9]+\.[0-9]*";
                            string Data = R[col].ToString();
                            Match w = Regex.Match(Data, Pattern);

                            if (w.Success)
                            {
                                lists.Add(Convert.ToInt64(Convert.ToDouble(Data)).ToString());
                            }

                            else if (col.ColumnName.Contains("DATE") && Data.ToString().Contains("\\"))
                                {

                                    string[] Dates = Data.ToString().Split('/');
                                    if (Dates[1].IndexOf("0") == 0)
                                    {

                                        Dates[1] = Dates[1].Substring(1);

                                    }

                                    if (Dates[2].IndexOf("0") == 0)
                                    {

                                        Dates[2] = Dates[2].Substring(1);

                                    }

                                    lists.Add(string.Join("/", new[] { Dates[1], Dates[2], Dates[0] }));

                                }

                            else if (col.ColumnName.Contains("DATE") && Data.ToString().Contains('-'))
                            {

                                string[] Dates = Data.ToString().Split('-');
                                if (Dates[1].IndexOf("0") == 0)
                                {

                                    Dates[1] = Dates[1].Substring(1);

                                }

                                if (Dates[2].IndexOf("0") == 0)
                                {

                                    Dates[2] = Dates[2].Substring(1);

                                }

                                lists.Add(string.Join("/", new[] { Dates[1], Dates[2], Dates[0] }));

                            }

                            else if (Data.ToLower() == "null")
                                {


                                    lists.Add("");
                                }

                                else
                                {

                                    lists.Add(Data.ToString().ToLower());

                                }
                            }
                        

                        var rec = string.Join(",", lists.ToArray());
                        
                        values.Add(rec);


                    }

                    Console.WriteLine(values[0]);

                    /*Hash DataRows*/

                    foreach (var value in values)
                    {
                        using (SHA1Managed sha1 = new SHA1Managed())
                        {

                            //Console.WriteLine("S3" + " " + value);


                            var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(value));
                            var sb = new StringBuilder(hash.Length * 2);



                            foreach (byte b in hash)
                            {
                                // convert to hexidecimal
                                sb.Append(b.ToString("X2"));
                            }


                            hashes.Add(sb.ToString());
                            //Console.WriteLine(sb.ToString());
                        }
                    }


                    Dt.Columns.Add(new DataColumn("hash"));



                    for (int i = 0; i < Dt.Rows.Count; i++)
                    {

                        Dt.Rows[i]["hash"] = hashes[i];
                        //Console.WriteLine("S3" + " " + hashes[i]);

                    }

                 
                    return Dt;

                    
                }
            }
            catch (IOException Ex)
            {
                Console.WriteLine("IO Error - {0}", Ex.Message.ToString());
                Console.ReadLine();
                return null;
            }
           

        }

        static void MatchOnly(DataTable dt, DataTable dt1, string Company, string StartDate, string EndDate, string ProcName, OdbcConnection Conn, int TableId, double sfexec)

        {
            
            string TestResult;
            string TestName = Company + "_" + StartDate + "_" + EndDate;
            string Today = DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss");
            string RowName = "RowCount";
            double SnowTime = sfexec;
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

                
                string Hash = Snow["hash"].ToString();
                var rec = string.Join("','", Snow.ItemArray.ToArray());
                Console.WriteLine(Hash);

                string InsertHash = "Insert into UnitTests Values('" + TestName + "','" + "RowHash" + "','" + Hash + "','" + " " + "','" + "Fail" + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";

                OdbcCommand InsertH = new OdbcCommand(InsertHash, Conn);

                InsertH.ExecuteNonQuery();


                string Insert = "Insert into " + ProcName + " Values('" + rec + "','" + Today + "','" + TestName + "')";
                
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

              /*  foreach (DataColumn col in Sql.Table.Columns)
                {
                    Console.WriteLine(col.ColumnName + " " + Sql[col].GetType());
                } */

                string Hash = Sql["hash"].ToString();
                var rec = string.Join("','", Sql.ItemArray.ToArray());
                Console.WriteLine(Hash);

                string InsertHash =  "Insert into UnitTests Values('" + TestName + "','" + "RowHash" + "','" + " " + "','" + Hash + "','" + "Fail" + "','" + Today + "','" + ProcName + "','" + TableId + "','" + SnowTime + "','" + SqlTime + "')";

                OdbcCommand InsertH = new OdbcCommand(InsertHash, Conn);

                InsertH.ExecuteNonQuery();

                string Insert = "Insert into " + ProcName + " Values('" + rec + "','" + Today + "','" + TestName + "')";

                OdbcCommand InsertData = new OdbcCommand(Insert, Conn);

                InsertData.ExecuteNonQuery();
            } 
        }

        static void Main(string[] args)
        {

            /*Sleep for 1.5 minute*/

            //Thread.Sleep(90000);
            DataTable Pdt = new DataTable();
            DataTable RawFile = new DataTable();
            string ProcName = "procExportDocumentLineDetail";
            StringBuilder SqlQuery = new StringBuilder("Select ");
            string ReportHash;

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

                        try

                        {
                            string command = "Select Parameters from nUnitTesting where ProcName = '" + ProcName + "'";

                            Console.WriteLine(command);

                            OdbcCommand GetValuesNames = new OdbcCommand(command, SnowConnection);

                            string Para = (string)GetValuesNames.ExecuteScalar();

                            string[] Parameters = Para.Split(',');

                            for (int NumParams = 1; NumParams < Parameters.Length; NumParams++)
                            {


                                SqlQuery.Append("Para" + NumParams + ", ");

                            }


                            /*With Parameters.Length, we can get the actual values for each of the parameters from the control table*/

                            SqlQuery.Append("Para" + Parameters.Length + " ,TableId,StageName" + " from nUnitTesting where ProcName = '" + ProcName + "'");

                            Console.WriteLine(SqlQuery.ToString());

                            /* for (int NumParams = 1; NumParams < Parameters.Length; NumParams++)
                             {


                                 SqlQuery.Append("Para" + NumParams + ", ");

                             }

                             /*Get the number of parameters for that procName

                            SqlQuery.Append("Para" + Parameters.Length + " from dbo.nUnitTesting where ProcName = '" + ProcName + "'");

                            Console.WriteLine(SqlQuery.ToString()); */



                            /*Select the parameter permutations and store them in Parameters data table*/

                            OdbcCommand GetParameterValues = new OdbcCommand(SqlQuery.ToString(), SnowConnection);

                            using (var Y = new OdbcDataAdapter(GetParameterValues))
                            {
                                Y.Fill(Pdt);

                            }

                            foreach (DataRow dr in Pdt.Rows)
                            {
                                DataTable Tab = new DataTable();
                                DataTable S3Tab = new DataTable();

                                string StartDate = dr["Para3"].ToString();
                                string EndDate = dr["Para4"].ToString();
                                string Company = dr["Para2"].ToString();
                                var TableId = Convert.ToInt32(dr["TableId"].ToString());
                                var StageName = dr["StageName"].ToString();

                                try
                                {

                                    string dir = @"C:\Users\franco.mang\Desktop\s3\";

                                    var sharedFile = new SharedCredentialsFile();

                                    sharedFile.TryGetProfile("default", out var profile);


                                    AWSCredentialsFactory.TryGetAWSCredentials(profile, sharedFile, out var credentials);



                                    AmazonS3Client s3Client = new AmazonS3Client(credentials);

                                    ReportHash = GetHash(SnowConnection, dr, ProcName, Parameters, StageName);

                                    string Hash = ReportHash.Split('-')[0];

                                    string path = dir + Hash + ".csv";

                                    var sfexec = Convert.ToDouble(ReportHash.Split('-')[1]);

                                    DownLoadFile(path, s3Client, Hash);

                                    Tab = QueryOps(Conn, dr, ProcName, Parameters);

                                    S3Tab = ExtractFile(path);

                                    Console.WriteLine("SqlCount" + " " + Tab.Rows.Count);
                                    Console.WriteLine("S3Count" + " " + S3Tab.Rows.Count);
                                    Console.ReadLine();


                                    //MatchOnly(Tab, S3Tab, Company, StartDate, EndDate, ProcName, TConnection, TableId, sfexec);

                                }
                                catch (Exception Ex)
                                {
                                    Console.WriteLine("AWS Error - {0}", Ex.Message.ToString());
                                    Console.WriteLine(Ex.ToString());
                                    Console.ReadLine();

                                }
                            }
                        }

                        catch (SqlException Ex)
                        {

                            Console.WriteLine(Ex.Message.ToString());
                            Console.ReadLine();

                        }



                    }
                }
            }
                }
            }
        }
       
            
