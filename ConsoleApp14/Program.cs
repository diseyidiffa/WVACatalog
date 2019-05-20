using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using MySql.Data.MySqlClient;
using System.Net;
using System.Net.Http;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp14
{
    class Program
    {
        private static string _url;
        private static string _fullPathWhereToSave;
        private static bool _result = false;
        private static string _filename;
        private static readonly SemaphoreSlim _semaphore = new SemaphoreSlim(0);
        private static Dictionary<string, string> dbColumn = new Dictionary<string, string>()
            {
                { "Description","description" },
                { "WVAProductKey", "wvaproductkey" },
                { "Base","base" },
                { "Diameter","diameter" },
                { "Sphere", "sphere" },
                { "Cylinder","cylinder" },
                { "Axis","axis" },
                { "Add","prdadd" },
                { "Color","color" },
                { "Multifocal","multifocal" },
                { "UPC","upc" },
                { "Modality","modality" },
                { "Vendor","vendor" },
                { "Rev-Diag","revdiag" },
                { "WVA_SKU","wvasku" }
            };
        static void Main(string[] args)
        {
            string path = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
            
            DataSet ds = TextToDataSet.Convert(@"C:\Users\d.diffa\Downloads\WVA_Contact_Lens_Catalog_Index.tsv", "data", "\t");

            int i = 1;
            foreach (DataRow row in ds.Tables["data"].Rows)
            {
                _filename = row["Prod_ID"].ToString() + ".tsv";
                _fullPathWhereToSave = System.IO.Path.Combine(path, _filename);
                if (File.Exists(_fullPathWhereToSave) && File.ReadAllBytes(_fullPathWhereToSave).Length > 0)
                {
                    
                    DataSet productDetails = TextToDataSet.Convert(_fullPathWhereToSave, "data", "\t");
                    fixColumns(productDetails);
                    Console.WriteLine("Processing.. " + _filename + " with " + productDetails.Tables["data"].Rows.Count + " File " + i + " of " + ds.Tables["data"].Rows.Count);
                    UpdateProductDetailsDB(productDetails);
                }
                i++;
            }
            Console.WriteLine("Done  - success: ");
            Console.ReadLine();
        }
        static void fixColumns( DataSet productDetails)
        {
            foreach (KeyValuePair<string, string> column in dbColumn)
            {
                if(!productDetails.Tables["data"].Columns.Contains(column.Key))
                {
                    productDetails.Tables["data"].Columns.Add(column.Value, typeof(string));
                }
            }
        }
        public static bool StartDownload(int timeout)
        {
            try
            {
                if (File.Exists(_fullPathWhereToSave))
                {
                    File.Delete(_fullPathWhereToSave);
                }
                using (WebClient client = new WebClient())
                {
                    var ur = new Uri(_url);
                    client.DownloadProgressChanged += WebClientDownloadProgressChanged;
                    client.DownloadFileCompleted += WebClientDownloadCompleted;
                    client.DownloadFileAsync(ur, _fullPathWhereToSave);
                    _semaphore.Wait(timeout);

                    return _result && File.Exists(_fullPathWhereToSave);
                }
            }
            catch (Exception e)
            {
                Console.Write("Was not able to download file!");
                //    Console.Write(e);
                return false;
            }
            finally
            {
                //     _semaphore.Dispose();
            }
        }
        private static void WebClientDownloadProgressChanged(object sender, DownloadProgressChangedEventArgs e)
        {
            Console.WriteLine(string.Format("{0} - {1}%.", _fullPathWhereToSave, e.ProgressPercentage));
        }

        private static void WebClientDownloadCompleted(object sender, AsyncCompletedEventArgs args)
        {
            _result = !args.Cancelled;
            if (!_result)
            {
                Console.Write(args.Error.ToString());
            }
            Console.Write("Download finished!");
            //  _semaphore.Release();
        }

        static void InsertProductIndex(DataSet ds)
        {
            const string DB_CONN_STR = "Server=127.0.0.1;Uid=root;Pwd=marcus24;Database=catalog;";
            StringBuilder sCommand = new StringBuilder("INSERT INTO productindex (prodid, description,vendor,startdate,comments,stopdate) VALUES ");



            using (MySqlConnection mConnection = new MySqlConnection(DB_CONN_STR))
            {
                List<string> Rows = new List<string>();
                foreach (DataRow row in ds.Tables["data"].Rows)
                {
                    string vendor =row["Vendor"].ToString();
                    string stopdateString = "NULL";
                    string startdateString = "NULL";


                    if (vendor.Equals("B & L")) vendor = "Bauch and Lomb";
                    if (row["Start_Date"].ToString().Length > 0)
                    {
                        DateTime startdate = Convert.ToDateTime(row["Start_Date"].ToString());
                        startdateString = startdate.Year + "-" + startdate.Month + "-" + startdate.Day;
                    }
                    if (row["Stop_Date"].ToString().Length > 0)
                    {
                        DateTime stopdate = Convert.ToDateTime(row["Stop_Date"].ToString());
                        stopdateString = stopdate.Year + "-" + stopdate.Month + "-" + stopdate.Day;
                    }

                    Rows.Add(string.Format("('{0}','{1}','{2}','{3}','{4}','{5}')", MySqlHelper.EscapeString(row["Prod_ID"].ToString()),
                                                                                    MySqlHelper.EscapeString(row["Description"].ToString()),
                                                                                    MySqlHelper.EscapeString(vendor),
                                                                                     MySqlHelper.EscapeString(startdateString),
                                                                                    MySqlHelper.EscapeString(row["Comments"].ToString()),
                                                                                     MySqlHelper.EscapeString(stopdateString)));
                }
                sCommand.Append(string.Join(",", Rows));
                sCommand.Append(";");
                mConnection.Open();
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }
        }
        
        static void UpdateProductDetailsDB(DataSet ds)
        {
            const string DB_CONN_STR = "Server=127.0.0.1;Uid=root;Pwd=marcus24;Database=catalog;";
            StringBuilder sCommand = new StringBuilder("INSERT INTO productdetails (description, wvaproductkey,base,diameter,sphere,cylinder,axis,prdadd,color,multifocal,upc,modality,vendor,revdiag,wvasku) VALUES ");

         

            using (MySqlConnection mConnection = new MySqlConnection(DB_CONN_STR))
            {
                mConnection.Open();
                List<string> Rows = new List<string>();
                int count = 1;
                foreach (DataRow row in ds.Tables["data"].Rows)
                {

                    string vendor = row["Vendor"].ToString();
                    if (vendor.Equals("B & L")) vendor = "Bauch and Lomb";


                    Rows.Add(string.Format("('{0}','{1}','{2}','{3}','{4}','{5}'," +
                                            "'{6}','{7}','{8}','{9}','{10}','{11}'," +
                                            "'{12}','{13}','{14}')", MySqlHelper.EscapeString(row["Description"].ToString()), 
                                                            MySqlHelper.EscapeString(row["WVAProductKey"].ToString()),
                                                            MySqlHelper.EscapeString(row["Base"].ToString()),
                                                            MySqlHelper.EscapeString(row["Diameter"].ToString()),
                                                            MySqlHelper.EscapeString(row["Sphere"].ToString()),
                                                            MySqlHelper.EscapeString(row["Cylinder"].ToString()),
                                                            MySqlHelper.EscapeString(row["Axis"].ToString()),
                                                            MySqlHelper.EscapeString(row["Add"].ToString()),
                                                            MySqlHelper.EscapeString(row["Color"].ToString()),
                                                            MySqlHelper.EscapeString(row["Multifocal"].ToString()),
                                                            MySqlHelper.EscapeString(row["UPC"].ToString()),
                                                            MySqlHelper.EscapeString(row["Modality"].ToString()),
                                                            MySqlHelper.EscapeString(vendor),
                                                            MySqlHelper.EscapeString(row["Rev-Diag"].ToString()),
                                                            MySqlHelper.EscapeString(row["WVA_SKU"].ToString()))
                            );
                    count++;
                    if (count > 20000)
                    {
                        InsertData(sCommand, mConnection, Rows);
                        Rows.Clear();
                        sCommand = new StringBuilder("INSERT INTO productdetails (description, wvaproductkey,base,diameter,sphere,cylinder,axis,prdadd,color,multifocal,upc,modality,vendor,revdiag,wvasku) VALUES ");
                        count = 1;
                    }
                }
                if (Rows.Count > 0)
                {
                    InsertData(sCommand, mConnection, Rows);
                }

            }

               
        }

        private static void InsertData(StringBuilder sCommand, MySqlConnection mConnection, List<string> Rows)
        {
            sCommand.Append(string.Join(",", Rows));
            sCommand.Append(";");
            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
            {
                myCmd.CommandType = CommandType.Text;
                myCmd.ExecuteNonQuery();
            }
        }
    }
}
