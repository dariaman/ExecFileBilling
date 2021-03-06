﻿using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class Program
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        static string FileResult = ConfigurationManager.AppSettings["DirResult"];
        static string FileBackup = ConfigurationManager.AppSettings["BackupResult"];

        static string FileBilling = ConfigurationManager.AppSettings["FileBilling"];
        static string BillingBackup = ConfigurationManager.AppSettings["BillingBackup"];
        static DateTime TglNow = DateTime.Now;

        //static string BCA_TEMP_TABLE = "UploadBcaCC";
        //static string MANDIRI_TEMP_TABLE = "UploadMandiriCC";
        //static string MEGAOnUs_TEMP_TABLE = "UploadMegaOnUsCC";
        //static string MEGAOffUs_TEMP_TABLE = "UploadMegaOfUsCC";
        //static string BNI_TEMP_TABLE = "UploadBniCC";

        static void Main(string[] args)
        {
            //args = new string[] { "exec" };
            //args = new string[] { "upload", "1" };
            if (!(args.Count() > 0))
            {
                Console.WriteLine("Parameter tidak terdefenisi...");
                Console.WriteLine("Aplication exit...");
                Thread.Sleep(10000);
                return;
            }
            if (args[0] == "upload")
            {
                if (!(args.Count() > 1))
                {
                    Console.WriteLine("Parameter Kurang...");
                    Console.WriteLine("Aplication exit...");
                    Thread.Sleep(10000);
                    return;
                }
                FileResultModel FileUpload;
                if (args[1] != "") // BCA CC Approve
                {
                    int idx = 0;
                    if (!int.TryParse(args[1], out idx))
                    {
                        Console.WriteLine("Parameter Salah");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(10000);
                        return;
                    }

                    FileUpload = getUploadFile(idx);
                    if (FileUpload.FileName == null)
                    {
                        Console.WriteLine("FileName Kosong");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(10000);
                        return;
                    }
                    if (!File.OpenRead(FileResult + FileUpload.FileName).CanRead)
                    {
                        Console.WriteLine("File tidak bisa di proses");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(10000);
                        return;
                    }

                    var IsData = CekFileInsert(idx, FileUpload.stageTable);
                    // Jika data sudah pernah diinsert atas file tersebut -> exit
                    if (CekFileInsert(idx, FileUpload.stageTable))
                    {
                        Console.WriteLine("Data Sudah Pernah insert . . . ");
                        Console.WriteLine("Aplication exit...");
                        Thread.Sleep(10000);
                        return;
                    }
                    else if (!(idx == 1 || idx == 2)) KosongkanTabel(FileUpload.stageTable);

                    List<DataUploadModel> DataUpload = new List<DataUploadModel>();
                    if (idx == 1)
                    {
                        // kosongkan table jika file bca yang satunya belum diupload (BCA Reject)
                        if (!CekFileInsert(2, FileUpload.stageTable)) KosongkanTabel(FileUpload.stageTable);
                        DataUpload = BacaFileBCA(FileUpload.FileName);
                    }
                    else if (idx == 2)
                    {
                        if (!CekFileInsert(1, FileUpload.stageTable)) KosongkanTabel(FileUpload.stageTable);
                        DataUpload = BacaFileBCA(FileUpload.FileName);
                    }
                    else if (idx == 3) DataUpload = BacaFileMandiri(FileUpload.FileName);
                    else if (idx == 4 || idx == 5) DataUpload = BacaFileMega(FileUpload.FileName);
                    else if (idx == 6) DataUpload = BacaFileBNI(FileUpload.FileName);

                    InsertTableStaging(DataUpload, FileUpload.stageTable, FileUpload.FileName);
                    MapingDataApprove(idx, FileUpload.stageTable);
                }
            }
            else if (args[0] == "exec")
            {
                ExceuteDataUpload();
            }
            else
            {
                Console.WriteLine("Nothing . . . ");
                Thread.Sleep(10000);
                return;
            }

            Console.WriteLine("F I N I S H . . . ");
            Thread.Sleep(10000);
        }

        public static Boolean CekFileInsert(int id, string tablename)
        {
            var Isdata = false;

            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT 1 
                        FROM `FileNextProcess` fp
                        INNER JOIN " + tablename + @" up ON up.`FileName`=fp.`FileName`
                        WHERE fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL AND fp.`id`=@idx
                        LIMIT 1;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });

            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read()) { Isdata = true; }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("CekFileInsert() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Isdata;
        }

        public static FileResultModel getUploadFile(int id)
        {
            FileResultModel Fileproses = new FileResultModel();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND id=@idx;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses = new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["FileBilling"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString()
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("genFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Fileproses;
        }

        public static List<FileResultModel> genFile()
        {
            List<FileResultModel> Fileproses = new List<FileResultModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND `tglProses` = CURDATE();", con);
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        Fileproses.Add(new FileResultModel()
                        {
                            Id = Convert.ToInt32(rd["id"]),
                            trancode = rd["trancode"].ToString(),
                            FileName = rd["FileName"].ToString(),
                            stageTable = rd["stageTable"].ToString(),
                            FileBilling = rd["FileBilling"].ToString(),
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString(),
                            tglSkrg= TglNow
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("genFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return Fileproses;
        }

        public static List<DataSubmitModel> PoolDataProsesApprove(int id, string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            Console.Write("Pooling data Approve ...");
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT u.* 
                                    FROM `FileNextProcess` fp
                                    INNER JOIN " + tableName + @" u ON u.`FileName`=fp.`FileName`
                                    WHERE fp.`id`=@idx AND fp.`FileName` IS NOT NULL AND fp.`tglProses` IS NOT NULL
                                    AND fp.`tglProses`=CURDATE() AND u.`IsExec`=0 AND u.`IsSukses`=1 AND u.BillCode='B'
                                    ORDER BY u.`PolisNo`,u.`seqid`;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@idx", MySqlDbType.Int32) { Value = id });

            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        DataProses.Add(new DataSubmitModel()
                        {
                            seqid = Convert.ToInt32(rd["seqid"]),
                            PolisNo = rd["PolisNo"].ToString(),
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            PolisId = rd["PolisId"].ToString(),
                            BillingID = (rd["BillingID"].ToString() == string.Empty) ? null : rd["BillingID"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
                            BillStatus = rd["BillStatus"].ToString(),
                            PolisStatus = rd["PolisStatus"].ToString(),
                            PremiAmount = Convert.ToDecimal(rd["PremiAmount"]),
                            CashlessFeeAmount = Convert.ToDecimal(rd["CashlessFeeAmount"]),
                            TotalAmount = Convert.ToDecimal(rd["TotalAmount"])
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("PoolDataProsesApprove(B) : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsExec`=0 AND u.`IsSukses`=1 AND u.BillCode<>'B';", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        DataProses.Add(new DataSubmitModel()
                        {
                            PolisNo = rd["PolisNo"].ToString(),
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            BillingID = (rd["BillingID"].ToString() == string.Empty) ? null : rd["BillingID"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("PoolDataProsesApprove(X) : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
            return DataProses;
        }

        static List<DataRejectModel> PoolDataProsesReject(string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            List<DataRejectModel> DataProses = new List<DataRejectModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=0 AND u.`BillingID` IS NOT NULL;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Clear();

            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        DataProses.Add(new DataRejectModel()
                        {
                            PolisNo = rd["PolisNo"].ToString(),
                            PolisId = (rd["BillCode"].ToString() == "B") ? Convert.ToInt32(rd["PolisId"]) : 0,
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            BillCode = rd["BillCode"].ToString(),
                            BillingID = rd["BillingID"].ToString(),
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("PoolDataProsesReject() : " + ex.Message);
                //Console.WriteLine("PoolDataProses Recurring Approve =>" + ex.Message);
            }
            finally
            {
                con.Close();
            }

            //Console.WriteLine("Finish");
            return DataProses;
        }

        public static void removeFile(FileResultModel Fileproses)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"UPDATE `FileNextProcess` SET `FileName`=NULL,`tglProses`=NULL WHERE `id`=@id;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.Id });
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
                FileInfo Filex = new FileInfo(FileResult + Fileproses.FileName);
                if (Filex.Exists) Filex.MoveTo(FileBackup + Fileproses.FileSaveName);
            }
            catch (Exception ex)
            {
                throw new Exception("removeFile() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

        }

        public static void removeFileBilling(FileResultModel Fileproses)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            try
            {
                con.Open();
                Decimal itemData = 0;
                cmd = new MySqlCommand(@"UpdateBillSum", con);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();

                cmd = new MySqlCommand(@"SELECT b.`TotalAmountDWD`+b.`TotalCountDWD` FROM `billing_download_summary` b WHERE b.`id`=@idd;", con);
                cmd.Parameters.Add(new MySqlParameter("@idd", MySqlDbType.Int32) { Value = Fileproses.id_billing_download });
                cmd.CommandType = CommandType.Text;
                var data = cmd.ExecuteScalar();

                if (!Decimal.TryParse(data.ToString(), out itemData)) return;
                if (itemData > 0) return;

                cmd = new MySqlCommand(@"UPDATE `FileNextProcess` SET `FileBilling`=NULL WHERE `id`=@id;", con);
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.Id });

                cmd.ExecuteNonQuery();

                FileInfo Filex = new FileInfo(FileBilling.Trim() + Fileproses.FileBilling.Trim());
                if (Filex.Exists) Filex.MoveTo(BillingBackup.Trim() + Fileproses.FileBilling.Trim() + Regex.Replace(Guid.NewGuid().ToString(), "[^0-9a-zA-Z]", "").Substring(0, 8));
            }
            catch (Exception ex)
            {
                throw new Exception("removeFile() : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void InsertTableStaging(List<DataUploadModel> DataUpload, string tableName, string FileName)
        {
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses,FileName) values ";
            string sql = "";
            int i = 0;
            foreach (DataUploadModel item in DataUpload)
            {
                if (item == null) continue;
                i++;
                sql = sql + string.Format(@"('{0}',{1},'{2}',NULLIF('{3}',''),'{4}','{5}',{6},'{7}'),",
                    item.PolisNo, item.Amount, item.ApprovalCode, item.Deskripsi, item.AccNo, item.AccName, item.IsSukses, FileName);
                // eksekusi per 100 data
                if (i == 100)
                {
                    ExecQueryAsync(sqlStart + sql.TrimEnd(','));
                    sql = "";
                    i = 0;
                }
            }
            //eksekusi sisanya 
            if (i > 0) ExecQueryAsync(sqlStart + sql.TrimEnd(','));
        }

        public static void ExecQueryAsync(string query)
        {
            using (MySqlConnection con = new MySqlConnection(constring))
            {
                MySqlCommand cmd = new MySqlCommand(query, con);
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.Text;
                try
                {
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    throw new Exception("ExecQueryAsync() : " + ex.Message);
                }
                finally
                {
                    con.Close();
                }
            }
        }

        public static List<DataUploadModel> BacaFileBCA(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses)))
            {
                string line;
                Decimal tmp1;
                while ((line = reader.ReadLine()) != null)
                {
                    var panjang = line.Length;
                    if (panjang < 171) continue;

                    if (!Decimal.TryParse(line.Substring(54, 9), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        PolisNo = line.Substring(9, 25).Trim(),
                        AccNo = line.Substring(34, 16).Trim(),
                        AccName = line.Substring(65, 26).Trim(),
                        Amount = tmp1,
                        ApprovalCode = (line.Substring(line.Length - 2) == "00")
                                        ? line.Substring(line.Length - 8).Substring(0, 6).Trim()
                                        : line.Substring(line.Length - 2),
                        Deskripsi = null,
                        IsSukses = (line.Substring(line.Length - 2) == "00") ? true : false,
                    });
                    //Console.WriteLine(line);
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMandiri(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if ((wb.Worksheets[1] == null) || (wb.Worksheets[2] == null)) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                Decimal tmp1;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 2].Value == null) || (ws.Cells[row, 3].Value == null) ||
                        (ws.Cells[row, 4].Value == null) || (ws.Cells[row, 7].Value == null) ||
                        (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 4].Value.ToString().Trim(),
                        PolisNo = ws.Cells[row, 6].Value.ToString().Trim(),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = true
                    });
                }

                // Sheet Reject (sheet 2) 
                ws = wb.Worksheets[2];
                startCell = ws.Dimension.Start;
                endCell = ws.Dimension.End;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    if ((ws.Cells[row, 2].Value == null) || (ws.Cells[row, 3].Value == null) ||
                        (ws.Cells[row, 4].Value == null) || (ws.Cells[row, 7].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        PolisNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        ApprovalCode = (ws.Cells[row, 5].Value ?? "").ToString().Trim(),
                        Deskripsi = (ws.Cells[row, 6].Value ?? "").ToString().Trim(),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
                fs.Close();
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMega(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if ((wb.Worksheets[1] == null) || (wb.Worksheets[2] == null)) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                Decimal tmp1;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 2].Value == null) ||
                        (ws.Cells[row, 3].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 5].Value == null) || (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    var temp = ws.Cells[row, 2].Value.ToString().Trim();
                    dataUpload.Add(new DataUploadModel()
                    {
                        //AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        PolisNo = temp.Split('-').Last().Trim(),
                        //AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = true
                    });
                }

                // Sheet Reject (sheet 2) 
                ws = wb.Worksheets[2];
                startCell = ws.Dimension.Start;
                endCell = ws.Dimension.End;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 2].Value == null) ||
                        (ws.Cells[row, 3].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 5].Value == null) || (ws.Cells[row, 6].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    var temp = ws.Cells[row, 2].Value.ToString().Trim();
                    dataUpload.Add(new DataUploadModel()
                    {
                        //AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        PolisNo = temp.Split('-').Last().Trim(),
                        //AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
            }
            return dataUpload;
        }
        public static List<DataUploadModel> BacaFileBNI(string Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                ExcelPackage xl = new ExcelPackage(fs);
                ExcelWorkbook wb = xl.Workbook;

                if (wb.Worksheets[1] == null) return null;
                // Sheet Approve (sheet 1) 
                ExcelWorksheet ws = wb.Worksheets[1];
                ExcelCellAddress startCell = ws.Dimension.Start;
                ExcelCellAddress endCell = ws.Dimension.End;

                Decimal tmp1;
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    // cek cell yang digunakan tidak null
                    if ((ws.Cells[row, 1].Value == null) || (ws.Cells[row, 4].Value == null) ||
                        (ws.Cells[row, 7].Value == null) || (ws.Cells[row, 8].Value == null) ||
                        (ws.Cells[row, 9].Value == null) || (ws.Cells[row, 10].Value == null)) continue;

                    // no urut
                    if (!Decimal.TryParse(ws.Cells[row, 1].Value.ToString().Trim(), out tmp1)) continue;
                    if (!Decimal.TryParse(ws.Cells[row, 8].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 9].Value.ToString().Trim(),
                        PolisNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        AccNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        AccName = ws.Cells[row, 5].Value.ToString().Trim(),
                        Deskripsi = ws.Cells[row, 10].Value.ToString().Trim(),
                        IsSukses = (ws.Cells[row, 9].Value.ToString().Trim() == "") ? false : true
                    });
                }
            }
            return dataUpload;
        }

        public static void KosongkanTabel(string TableName)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE FROM " + TableName + ";", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("KosongkanTabel() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void MapingDataApprove(int idx, string tableName)
        {
            Console.WriteLine("Mapping data  ...");
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;

            try
            {
                con.Open();

                cmd = new MySqlCommand(@"UPDATE " + tableName + " SET `seqid`=NULL,`BillingID`=NULL;", con);
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("MapingDataApprove() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            try
            {
                cmd = new MySqlCommand(@"
SET @prev_value := 0;
SET @rank_count := 0;
DROP TEMPORARY TABLE IF EXISTS billx;
CREATE TEMPORARY TABLE billx AS	
SELECT 
CASE
    WHEN @prev_value = b.policy_id THEN @rank_count := @rank_count + 1
    WHEN @prev_value := b.policy_id THEN @rank_count:=1
END AS seqno,
	b.policy_id,
	policy_no,
	b.BillingID
FROM `billing` b
INNER JOIN `policy_billing` pb ON pb.policy_Id=b.policy_id
INNER JOIN (
	SELECT DISTINCT s.`PolisNo` FROM " + tableName + @" s #WHERE s.`IsSukses`=1
) su ON su.`PolisNo`=pb.`policy_no`
WHERE b.status_billing IN ('A','C') 
ORDER BY b.policy_id,b.recurring_seq;
		
UPDATE " + tableName + @" up
SET up.`seqid`=CASE
	    WHEN @prev_value = up.`PolisNo` THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := up.`PolisNo` THEN @rank_count:=1
	END
WHERE LEFT(up.`PolisNo`,1) NOT IN ('A','X') #AND up.`IsSukses`=1
ORDER BY up.`PolisNo`,up.`Amount`;

## Maping data upload yang ada billingnya
update " + tableName + @" up
inner join billx bx on up.`PolisNo`=bx.`policy_no` and up.`seqid`=bx.seqno
inner join `policy_billing` pb on pb.`policy_Id`=bx.policy_id
inner join `billing` b on b.`BillingID`=bx.BillingID
SET up.`PolisId`=pb.`policy_Id`,
	up.`PolisStatus`=pb.`Policy_status`,
	up.`BillingID`=b.`BillingID`,
	up.`BillStatus`=b.`status_billing`,
	up.`BillCode`='B',
	up.`PremiAmount`=b.`policy_regular_premium`,
	up.`CashlessFeeAmount`=b.`cashless_fee_amount`,
	up.`TotalAmount`=b.`TotalAmount`;
#WHERE up.`IsSukses`=1;

## isi data yang gak ada billing (karena akan create billing)
UPDATE " + tableName + @" up
INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
	SET up.`PolisId`=pb.`policy_Id`,
	up.`BillCode`='B',
	up.`PremiAmount`=pb.`regular_premium`,
	up.`CashlessFeeAmount`=pb.`cashless_fee_amount`,
	up.`TotalAmount`=pb.`regular_premium`+pb.`cashless_fee_amount`
WHERE up.`PolisId` IS NULL AND up.`IsSukses`=1
#AND up.`IsSukses`=1
AND LEFT(up.`PolisNo`,1) NOT IN ('A','X');


UPDATE " + tableName + @" up
INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`PolisNo` AND bo.`status_billing` NOT IN ('P','R')
	SET up.`BillingID`=up.`PolisNo`,up.`BillCode`='A'
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1)='A';

UPDATE " + tableName + @" up
INNER JOIN `quote_billing` q ON q.`quote_id`=SUBSTRING(up.`PolisNo`,2) AND q.`status` NOT IN ('P','R')
	SET up.`BillingID`=SUBSTRING(up.`PolisNo`,2),
	up.`BillCode`='Q'
WHERE LEFT(up.`PolisNo`,1)='X'; #AND up.`IsSukses`=1;", con);
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.Text;

                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("MapingDataApprove() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        //        public static void MapingDataReject(string tableName)
        //        {
        //            Console.Write("Mapping data Reject ...");
        //            MySqlConnection con = new MySqlConnection(constring);
        //            MySqlCommand cmd;
        //            cmd = new MySqlCommand(@"
        //SET @prev_value := 0;
        //SET @rank_count := 0;
        //DROP TEMPORARY TABLE IF EXISTS billx;
        //CREATE TEMPORARY TABLE billx AS	
        //SELECT CASE
        //	    WHEN @prev_value = b.policy_id THEN @rank_count := @rank_count + 1
        //	    WHEN @prev_value := b.policy_id THEN @rank_count:=1
        //	END AS seqno,
        //	b.policy_id,
        //	b.policy_no,
        //	b.BillingID
        //FROM (
        //	SELECT DISTINCT pb.`policy_no`,pb.`policy_Id`,b.`BillingID`
        //	FROM " + tableName + @" up
        //	INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
        //	INNER JOIN `billing` b ON pb.`policy_Id`=b.`policy_id` AND b.`status_billing` IN ('A','C')
        //    WHERE SUBSTR(up.`PolisNo`,0,1) NOT IN ('A','X')
        //)b;

        //SET @prev_value := 0;
        //SET @rank_count := 0;
        //DROP TEMPORARY TABLE IF EXISTS billu;
        //CREATE TEMPORARY TABLE billu AS	
        //SELECT 
        //	CASE
        //	    WHEN @prev_value = up.`PolisNo` THEN @rank_count := @rank_count + 1
        //	    WHEN @prev_value := up.`PolisNo` THEN @rank_count:=1
        //	END AS seqno,
        //	up.`id`, up.`PolisNo`
        //FROM " + tableName + @" up
        //WHERE up.`IsSukses`=0 AND SUBSTR(up.`PolisNo`,0,1) NOT IN ('A','X')
        //ORDER BY up.`PolisNo`,up.`Amount`;

        //UPDATE " + tableName + @" up
        //INNER JOIN billu bu ON up.`id`=bu.id
        //INNER JOIN billx b ON bu.seqno=b.seqno AND bu.PolisNo=b.policy_no
        //SET up.`PolisId`=b.policy_id, 
        //up.`BillCode`='B',
        //up.`BillingID`=b.BillingID
        //WHERE SUBSTR(up.`PolisNo`,0,1) NOT IN ('A','X');

        //UPDATE " + tableName + @" up 
        //INNER JOIN `billing_others` bo ON bo.`BillingID`=up.`PolisNo`
        //SET up.`PolisId`=bo.`policy_id`,up.`BillCode`='A',up.`BillingID`=up.`PolisNo`
        //WHERE SUBSTR(up.`PolisNo`,1,1)='A';

        //UPDATE " + tableName + @" up SET up.`BillingID`=SUBSTR(up.`PolisNo`,2),up.`BillCode`='Q'
        //WHERE SUBSTR(up.`PolisNo`,1,1)='X';
        //", con);
        //            cmd.Parameters.Clear();
        //            cmd.CommandType = CommandType.Text;
        //            try
        //            {
        //                con.Open();
        //                cmd.ExecuteNonQuery();
        //            }
        //            catch (Exception ex)
        //            {
        //                throw new Exception("MapingDataReject() : " + ex.Message);
        //            }
        //            finally
        //            {
        //                con.CloseAsync();
        //            }
        //        }

        public static void SubmitApproveTransaction(string tableName, List<DataSubmitModel> DataProses, FileResultModel DataHeader)
        {
            int i = 0;
            Console.WriteLine();
            foreach (DataSubmitModel item in DataProses)
            {
                i++;
                try
                {
                    Console.Write(String.Format("{0} ", i));
                    if (item.BillCode == "B") RecurringApprove(item, DataHeader);
                    else if (item.BillCode == "A") BillOtherApprove(item, DataHeader);
                    else if (item.BillCode == "Q") QuoteApprove(item, DataHeader);
                    Console.WriteLine(String.Format("PolisNo {0} ", item.PolisNo));
                }
                catch (Exception ex)
                {
                    throw new Exception("SubmitApproveTransaction() : " + ex.Message);
                }
            }
        }

        public static void RecurringApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.PolisId == null) || (DataProses.PolisId == "")) return;

            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Create Billing jika billing tidak ada pada saat mapping (karena Approve)
                if ((DataProses.BillingID == null) || (DataProses.BillingID == ""))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"CreateNewBillingRecurring";
                    cmd.Parameters.Add(new MySqlParameter("@polisId", MySqlDbType.VarChar) { Value = DataProses.PolisId });
                    DataProses.BillingID = cmd.ExecuteScalar().ToString();
                }

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `jbsdb`.`transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@PolicyId,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileSaveName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@PolicyId", MySqlDbType.VarChar) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();
                //Console.Write(String.Format("TransHistory={0} ... ", DataProses.TransHistory));

                // Insert Receipt
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt`(`receipt_date`,`receipt_policy_id`, `receipt_fund_type_id`, `receipt_transaction_code`, `receipt_amount`,
`receipt_source`, `receipt_status`, `receipt_payment_date_time`, `receipt_seq`, `bank_acc_id`, `due_date_pre`,`acquirer_bank_id`)
SELECT @tgl,up.`PolisId`,0,'RP',up.`Amount`-b.`cashless_fee_amount`,@source,'P',@tgl,b.`recurring_seq`,@bankAccId,b.`due_dt_pre`,@bankid
FROM " + DataHeader.stageTable + @" up
LEFT JOIN `billing` b ON b.`BillingID`=@Billid
WHERE up.`seqid`=@SeqId AND up.`PolisNo`=@PolisNo;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@SeqId", MySqlDbType.Int32) { Value = DataProses.seqid });
                cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                cmd.Parameters.Add(new MySqlParameter("@Billid", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                DataProses.receiptID = cmd.ExecuteScalar().ToString();
                //Console.Write(String.Format("receiptID={0} ... ", DataProses.receiptID));

                // Insert Receipt Other
                if (DataProses.CashlessFeeAmount > 0)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
SELECT @tgl,b.`policy_id`,3,b.`cashless_fee_amount`,@source,@tgl,b.`recurring_seq`,@bankAccId,@bankid
FROM " + DataHeader.stageTable + @" up
LEFT JOIN `billing` b ON b.`BillingID`=@Billid
WHERE up.`seqid`=@SeqId AND up.`PolisNo`=@PolisNo;
SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@SeqId", MySqlDbType.Int32) { Value = DataProses.seqid });
                    cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                    cmd.Parameters.Add(new MySqlParameter("@Billid", MySqlDbType.Int32) { Value = DataProses.BillingID });
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                    cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                    cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                    DataProses.receiptOtherID = cmd.ExecuteScalar().ToString();
                }

                // Insert CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`policy_cc_transaction`(`policy_id`,`transaction_dt`,`transaction_type`,`recurring_seq`,
`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,`acquirer_bank_id`,
`cc_no`,`cc_name`,`status_id`,`remark`,`receipt_id`,`receipt_other_id`,`created_dt`)
SELECT up.`PolisId`,@tgl,'R',b.`recurring_seq`,1,'IDR',b.`TotalAmount`,b.`due_dt_pre`,DATE_FORMAT(b.`due_dt_pre`,'%b%d'),@bankid,
COALESCE(NULLIF(up.`AccNo`,''),NULLIF(b.`AccNo`,''),pc.`cc_no`),COALESCE(NULLIF(up.`AccName`,''),NULLIF(b.`AccName`,''),pc.`cc_name`),
2,'APPROVED',@receiptID,@receiptOtherID,@tgl
FROM " + DataHeader.stageTable + @" up
INNER JOIN `billing` b ON b.`BillingID`=@BillID
LEFT JOIN `jbsdb`.`policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
WHERE up.`seqid`=@seqid AND up.`PolisNo`=@PolisNo;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptID });
                cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@BillID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@seqid", MySqlDbType.Int32) { Value = DataProses.seqid });
                cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                DataProses.TransID = cmd.ExecuteScalar().ToString();
                //Console.Write(String.Format("TransID={0} ... ", DataProses.TransID));

                // Update Billing JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `jbsdb`.`billing` b
                                    LEFT JOIN `jbsdb`.`policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
                                        SET b.`IsDownload`=0,
                                        b.`IsClosed`=1,
                                        b.`BillingDate`=COALESCE(b.`BillingDate`,@tgl),
                                        b.`status_billing`='P',
                                        b.`PaymentSource`='CC',
                                        b.`BankIdPaid`=@bankid,
                                        b.`PaidAmount`=@PaidAmount,
                                        b.`Life21TranID`=@TransactionID,
                                        b.`ReceiptID`=@receiptID,
                                        b.`ReceiptOtherID`=@ReceiptOtherID,
                                        b.`PaymentTransactionID`=@uid,
                                        b.`ACCname`=COALESCE(NULLIF(@ACCname,''),NULLIF(`ACCname`,''),pc.`cc_name`),
                                        b.`ACCno`=COALESCE(NULLIF(@ACCno,''),NULLIF(`ACCno`,''),pc.`cc_no`),
                                        b.`cancel_date`=null,
                                        b.`LastUploadDate`=@tgl,
                                        b.`UserUpload`='system'
                                    WHERE b.`BillingID`=@idBill;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@TransactionID", MySqlDbType.Int32) { Value = DataProses.TransID });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptID });
                cmd.Parameters.Add(new MySqlParameter("@ReceiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.Parameters.Add(new MySqlParameter("@ACCname", MySqlDbType.VarChar) { Value = DataProses.AccName });
                cmd.Parameters.Add(new MySqlParameter("@ACCno", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Update Polis Last Transaction JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"INSERT INTO `jbsdb`.`policy_last_trans`(`policy_Id`,`BillingID`,`BillingDate`,`recurring_seq`,`due_dt_pre`,`source`,`receipt_id`,`receipt_date`,`bank_id`,`UserCrt`)
                            SELECT bx.policy_id, bx.`BillingID`,bx.`BillingDate`,bx.`recurring_seq`,bx.`due_dt_pre`,bx.`PaymentSource`,bx.`ReceiptID`,@tgl,bx.`BankIdDownload`,@usercrt
                            FROM `billing` AS bx
                            LEFT JOIN `policy_last_trans` AS pt ON bx.policy_id=pt.policy_Id
                            WHERE bx.BillingID=@idBill
                            ON DUPLICATE KEY UPDATE `BillingID`=bx.`BillingID`,
	                            `BillingDate`=bx.`BillingDate`,
	                            `recurring_seq`=bx.`recurring_seq`,
	                            `due_dt_pre`=bx.`due_dt_pre`,
	                            `source`=bx.`PaymentSource`,
	                            `receipt_id`=bx.`ReceiptID`,
	                            `receipt_date`=@tgl,
	                            `bank_id`=bx.`BankIdDownload`,
	                            `UserCrt`='system';";
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.ExecuteNonQuery();

                // Flaging data upload staging
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up
                                SET up.`IsExec`=1
                                WHERE up.`PolisNo`=@polisno AND up.`seqid`=@seqid AND up.`BillCode`='B'";
                cmd.Parameters.Add(new MySqlParameter("@polisno", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                cmd.Parameters.Add(new MySqlParameter("@seqid", MySqlDbType.Int32) { Value = DataProses.seqid });
                cmd.ExecuteNonQuery();

                // Insert EmailQuee
                var emailThanks = new EmailThanksRecuring(Convert.ToInt32(DataProses.BillingID), DataProses.Amount, DataHeader.tglSkrg);
                emailThanks.InsertEmailQuee();
                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "RecurringApprove : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void BillOtherApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.BillingID == null) || (DataProses.BillingID == "")) return;

            //Console.Write(String.Format("Polis {0} ...", DataProses.PolisNo));

            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `jbsdb`.`transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@PolicyId,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileSaveName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@PolicyId", MySqlDbType.VarChar) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();
                //Console.Write(String.Format("TransHistory={0} ... ", DataProses.TransHistory));

                // Insert Receipt Other
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
SELECT @tgl,b.`policy_id`,1,b.`TotalAmount`,@source,@tgl,0,@bankAccId,@bankid
FROM " + DataHeader.stageTable + @" up
INNER JOIN `billing_others` b ON b.`BillingID`=up.`BillingID`
WHERE up.`BillingID`=@PolisNo;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                DataProses.receiptOtherID = cmd.ExecuteScalar().ToString();
                //Console.Write(String.Format("receiptOtherID={0} ... ", DataProses.receiptOtherID));

                // Update Life21 CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21`.`policy_cc_transaction` pc
                                INNER JOIN `billing_others` bo ON bo.`Life21TranID`=pc.`policy_cc_tran_id`
                                    SET pc.status_id=2,
	                                pc.result_status=@rstStatus,
	                                pc.Remark='APPROVED',
	                                pc.receipt_other_id=@receiptID,
	                                pc.update_dt=@tgl
                                    WHERE bo.`BillingID`=@id;";
                cmd.Parameters.Add(new MySqlParameter("@rstStatus", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Update Billing Other JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `billing_others` SET `IsDownload`=0,
			                                `IsClosed`=1,
			                                `status_billing`='P',
                                            `PaymentSource`='CC',
			                                `LastUploadDate`=@tgl,
                                            `paid_date`=DATE(@tgl),
                                            BankIdPaid=@bankid,
                                            `PaidAmount`=@PaidAmount,
			                                `ReceiptOtherID`=@receiptOtherID,
			                                `PaymentTransactionID`=@uid,
                                            UserUpload='system'
		                                WHERE `BillingID`=@idBill;";

                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = DataProses.receiptOtherID });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.VarChar) { Value = DataProses.TransHistory });
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Flaging data upload staging
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up 
                                SET up.`IsExec`=1
                                WHERE up.`PolisNo`=@polisno AND up.`BillCode`='A'";
                cmd.Parameters.Add(new MySqlParameter("@polisno", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                cmd.ExecuteNonQuery();

                var emailEndorsThanks = new EmailThanksEndorsemen(DataProses.BillingID, DataProses.Amount, DataHeader.tglSkrg);
                emailEndorsThanks.InsertEmailQuee();

                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "BillOtherApprove : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void QuoteApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.BillingID == null) || (DataProses.BillingID == "")) return;
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();
            try
            {
                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `transaction_bank`(`File_Backup`,`TranCode`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileSaveName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.VarChar) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();

                // Update status Quote jadi Paid
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`quote` q
                                        SET q.`quote_status`='P',
                                        quote_submitted_dt=@tgl
                                        WHERE q.`quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQueryAsync().Wait();

                // Update Prospect Billing jadi approve
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`prospect_billing`
                                        SET prospect_convert_flag=2,prospect_appr_code='UP4Y1',
                                        updated_dt=@tgl,
                                        acquirer_bank_id=@bankid
                                        WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Update quote_edc jadi approve
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `prod_life21p`.`quote_edc`
                                        SET status_id=1,
                                        reason='',
                                        appr_code='UP4Y1'
                                        WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.ExecuteNonQuery();

                // Update Quote JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `quote_billing` SET `IsDownload`=0,
			                                    `IsClosed`=1,
			                                    `status`='P',
                                                `PaymentSource`='CC',
                                                `PaidAmount`=@PaidAmount,
                                                BankIdPaid=@bankid,
			                                    `LastUploadDate`=@tgl,
                                                `cancel_date`=null,
                                                `paid_dt`=DATE(@tgl),
			                                    `PaymentTransactionID`=@uid,
                                                UserUpload='system'
		                                    WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.ExecuteNonQuery();

                // Flaging data upload staging
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE " + DataHeader.stageTable + @" up 
                                SET up.`IsExec`=1
                                WHERE up.`PolisNo`=@polisno AND up.`BillCode`='Q'";
                cmd.Parameters.Add(new MySqlParameter("@polisno", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                cmd.ExecuteNonQuery();

                // Insert EmailQuee
                var emailQuoteThanks = new EmailThanksQuote(Convert.ToInt32(DataProses.BillingID), DataProses.Amount, DataHeader.tglSkrg);
                emailQuoteThanks.InsertEmailQuee();

                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                var LogError = new ErrorLog(DataHeader.trancode, DataProses.PolisNo, DataProses.IsSukses, "QuoteApprove : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }

        public static void SubmitRejectTransaction(string tableName, FileResultModel DataHeader)
        {
            Console.WriteLine("RejectTransaction Begin ....");

            Console.Write("Reject Recurring ....");
            RejectBillingTransaction(tableName, DataHeader);
            Console.WriteLine(">> Done");

            Console.Write("Reject Billing Others ....");
            RejectBillingOthersTransaction(tableName, DataHeader);
            Console.WriteLine(">> Done");

            Console.Write("Reject quote ....");
            RejectQuoteTransaction(tableName, DataHeader);
            Console.WriteLine(">> Done");
        }

        public static void RejectBillingTransaction(string tableName, FileResultModel DataHeader)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand();
            cmd.Parameters.Clear();
            try
            {
                con.Open();
                cmd.Connection = con;
                cmd.CommandText = @"
SELECT `AUTO_INCREMENT` INTO @tbid
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'jbsdb' AND TABLE_NAME='transaction_bank';

INSERT INTO transaction_bank(`File_Backup`,`TranCode`,`TranDate`,`IsSuccess`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
SELECT @FileName,@Trancode,@tgl,up.`IsSukses`,up.`PolisId`,up.`BillingID`,up.`Amount`,up.`ApprovalCode`,
COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`), up.`Deskripsi`),
COALESCE(up.`AccNo`,b.`AccNo`,pc.`cc_no`),COALESCE(up.`AccName`,b.`AccName`,pc.`cc_name`)
FROM " + tableName + @" up
INNER JOIN `billing` b ON b.`BillingID`=up.`BillingID` 
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode`
LEFT JOIN `policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
WHERE up.`IsSukses`=0 AND up.`BillCode`='B' AND up.`BillingID` IS NOT NULL;

UPDATE `billing` b
INNER JOIN " + tableName + @" up ON up.`BillingID`=b.`BillingID`
LEFT JOIN transaction_bank tb ON tb.`BillingID`=up.`BillingID` AND tb.`id` >= @tbid
	SET b.`IsDownload`=0,
	b.`LastUploadDate`=@tgl,
	b.`PaymentTransactionID`=tb.`id`,
	b.`BankIdDownload`=@BankIdDwD,
	b.`Source_download`='CC',
	b.`BillingDate`=COALESCE(b.`BillingDate`,@tgl)
WHERE b.`status_billing` IN ('A','C') AND up.`BillingID` IS NOT NULL AND up.`BillCode`='B'
;";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@BankIdDwD", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@FileName", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@Trancode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("RejectBillingTransaction() : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void RejectBillingOthersTransaction(string tableName, FileResultModel DataHeader)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand();
            cmd.Parameters.Clear();
            try
            {
                con.Open();
                cmd.Connection = con;
                cmd.CommandText = @"
SELECT `AUTO_INCREMENT` INTO @tbid
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'jbsdb' AND TABLE_NAME='transaction_bank';

INSERT INTO transaction_bank(`File_Backup`,`TranCode`,`TranDate`,`IsSuccess`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
SELECT @FileName,@Trancode,@tgl,up.`IsSukses`,up.`PolisId`,up.`BillingID`,up.`Amount`,up.`ApprovalCode`,
COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`), up.`Deskripsi`),
COALESCE(up.`AccNo`,b.`AccNo`,pc.`cc_no`),COALESCE(up.`AccName`,b.`AccName`,pc.`cc_name`)
FROM " + tableName + @" up
INNER JOIN `billing_others` b ON b.`BillingID`=up.`PolisNo`
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode`
LEFT JOIN `policy_cc` pc ON pc.`PolicyId`=b.`policy_id`
WHERE up.`IsSukses`=0 AND up.`BillCode`='A' AND up.`BillingID` IS NOT NULL;

UPDATE `billing_others` q
INNER JOIN " + tableName + @" up ON up.`PolisNo`=q.`BillingID`
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`BillingID`
	SET q.`IsDownload`=0,
        q.`LastUploadDate`=@tgl,
        q.`PaymentTransactionID`=tb.`id`
WHERE q.`status_billing` IN ('A','C') AND up.`BillingID` IS NOT NULL AND up.`BillCode`='A'
	AND tb.`id` >= @tbid;
";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@BankIdDwD", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@FileName", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@Trancode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("RejectBillingTransaction() : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void RejectQuoteTransaction(string tableName, FileResultModel DataHeader)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd = new MySqlCommand();
            cmd.Parameters.Clear();
            try
            {
                con.Open();
                cmd.Connection = con;
                cmd.CommandText = @"
SELECT `AUTO_INCREMENT` INTO @tbid
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'jbsdb' AND TABLE_NAME='transaction_bank';

INSERT INTO transaction_bank(`File_Backup`,`TranCode`,`TranDate`,`IsSuccess`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
SELECT @FileName,@Trancode,@tgl,up.`IsSukses`,up.`PolisId`,up.`BillingID`,up.`Amount`,up.`ApprovalCode`,
COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`), up.`Deskripsi`),
COALESCE(up.`AccNo`,q.`acc_no`),COALESCE(up.`AccName`,q.`acc_name`)
FROM " + tableName + @" up
INNER JOIN `quote_billing` q ON q.`quote_id`=up.`BillingID`
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode`
WHERE up.`IsSukses`=0 AND up.`BillCode`='Q' AND up.`BillingID` IS NOT NULL;

UPDATE `quote_billing` q
INNER JOIN " + tableName + @" up ON up.`BillingID`=q.`quote_id`
INNER JOIN transaction_bank tb ON tb.`BillingID`=q.`quote_id`
	SET q.`IsDownload`=0,
        q.`LastUploadDate`=@tgl,
        q.`PaymentTransactionID`=tb.`id`
WHERE q.`status` IN ('A','C') AND up.`BillingID` IS NOT NULL AND up.`BillCode`='Q'
	AND tb.`id` >= @tbid;
";
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@BankIdDwD", MySqlDbType.Int32) { Value = DataHeader.bankid });
                cmd.Parameters.Add(new MySqlParameter("@FileName", MySqlDbType.VarChar) { Value = DataHeader.FileName });
                cmd.Parameters.Add(new MySqlParameter("@Trancode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("RejectBillingTransaction() : " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void ExceuteDataUpload()
        {
            Boolean stop = true;
            List<DataSubmitModel> DataProses;
            while (stop)
            {
                try
                {
                    var Fileproses = genFile();
                    foreach (FileResultModel item in Fileproses)
                    {
                        item.FileSaveName = item.FileName + Guid.NewGuid().ToString().Substring(0, 8);
                        DataProses = new List<DataSubmitModel>();
                        MapingDataApprove(item.Id, item.stageTable);
                        DataProses = PoolDataProsesApprove(item.Id, item.stageTable);
                        if (DataProses.Count > 0) SubmitApproveTransaction(item.stageTable, DataProses, item);

                        //Proses yang reject
                        //MapingDataReject(item.stageTable);
                        if(item.Id !=2) SubmitRejectTransaction(item.stageTable, item);

                        removeFile(item);
                        removeFileBilling(item);
                    }

                    stop = false;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Thread.Sleep(10000);
                }
            }
        }
    }
}
