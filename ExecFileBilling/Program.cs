﻿using MySql.Data.MySqlClient;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class Program
    {
        static string constring = ConfigurationManager.AppSettings["DefaultDB"];
        static string FileResult = ConfigurationManager.AppSettings["DirResult"];
        static string FileBackup = ConfigurationManager.AppSettings["BackupResult"];
        //static DateTime tglSekarang = DateTime.Now;

        static string BCA_TEMP_TABLE = "UploadBcaCC";
        static string MANDIRI_TEMP_TABLE = "UploadMandiriCC";
        static string MEGAOnUs_TEMP_TABLE = "UploadMegaOnUsCC";
        static string MEGAOffUs_TEMP_TABLE = "UploadMegaOfUsCC";
        static string BNI_TEMP_TABLE = "UploadBniCC";

        static void Main(string[] args)
        {
            var Fileproses = genFile();
            List<DataUploadModel> DataUpload;
            List<DataSubmitModel> DataProses;
            List<DataRejectModel> DataReject;

            DateTime tglSekarang = DateTime.Now;

            foreach (FileResultModel item in Fileproses)
            {
                item.FileSaveName = item.FileName + Guid.NewGuid().ToString().Substring(0, 8);
                item.tglSkrg = DateTime.Now;
                DataUpload = new List<DataUploadModel>();
                DataProses = new List<DataSubmitModel>();
                DataReject = new List<DataRejectModel>();
                Console.WriteLine(item.FileName);
                switch (item.Id)
                {
                    case 1: // BCA Approve
                        //KosongkanTabel();
                        //DataUpload = BacaFileBCA(item);
                        //InsertTableStaging(DataUpload, BCA_TEMP_TABLE);
                        //MapingDataApprove(BCA_TEMP_TABLE);
                        //DataProses = PoolDataProsesApprove(BCA_TEMP_TABLE);
                        //SubmitApproveTransaction(BCA_TEMP_TABLE, DataProses, item);
                        break;
                    case 2: // BCA Reject
                        //KosongkanTabel();
                        //DataUpload = BacaFileBCA(item);
                        //InsertTableStaging(DataUpload, BCA_TEMP_TABLE);
                        //MapingDataReject(BCA_TEMP_TABLE);
                        //DataReject = PoolDataProses(BCA_TEMP_TABLE, 0);
                        //SubmitRejectTransaction(BCA_TEMP_TABLE, DataReject, item);
                        break;
                    case 3: // Mandiri
                        //KosongkanTabel();
                        //DataUpload = BacaFileMandiri(item);
                        //InsertTableStaging(DataUpload, MANDIRI_TEMP_TABLE);
                        //MapingDataApprove(MANDIRI_TEMP_TABLE);
                        //DataProses = PoolDataProsesApprove(MANDIRI_TEMP_TABLE);
                        //SubmitApproveTransaction(MANDIRI_TEMP_TABLE, DataProses, item);

                        // Proses yang reject
                        //DataReject = PoolDataProsesReject(MANDIRI_TEMP_TABLE);
                        //SubmitRejectTransaction(MANDIRI_TEMP_TABLE, DataProses, item);
                        break;
                    case 4:
                        //KosongkanTabel();
                        //DataUpload = BacaFileMega(item);
                        //InsertTableStaging(DataUpload, MEGAOnUs_TEMP_TABLE);
                        //MapingDataApprove(MEGAOnUs_TEMP_TABLE);
                        //DataProses = PoolDataProses(MEGAOnUs_TEMP_TABLE, 1);
                        //SubmitApproveTransaction(MEGAOnUs_TEMP_TABLE, DataProses, item);
                        break;
                    case 5:
                        //KosongkanTabel();
                        //DataUpload = BacaFileMega(item);
                        //InsertTableStaging(DataUpload, MEGAOffUs_TEMP_TABLE);
                        //MapingDataApprove(MEGAOffUs_TEMP_TABLE);
                        //DataProses = PoolDataProses(MEGAOffUs_TEMP_TABLE, 1);
                        //SubmitApproveTransaction(MEGAOffUs_TEMP_TABLE, DataProses, item);
                        break;
                    case 6: // BNI
                        KosongkanTabel();
                        DataUpload = BacaFileBNI(item);
                        InsertTableStaging(DataUpload, BNI_TEMP_TABLE);
                        MapingDataApprove(BNI_TEMP_TABLE);
                        DataProses = PoolDataProsesApprove(BNI_TEMP_TABLE);
                        SubmitApproveTransaction(BNI_TEMP_TABLE, DataProses, item);

                        //// Proses yang reject
                        //DataReject = PoolDataProsesReject(BNI_TEMP_TABLE);
                        //SubmitRejectTransaction(BNI_TEMP_TABLE, DataProses, item);
                        break;
                }
                //removeFile(item);
            }

            Console.WriteLine((DateTime.Now - tglSekarang).Seconds.ToString());
            Console.WriteLine("Selesai . . .");
            Console.ReadKey();
        }

        public static List<FileResultModel> genFile()
        {
            List<FileResultModel> Fileproses = new List<FileResultModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `FileNextProcess`
                                    WHERE `FileName` IS NOT NULL AND `tglProses` IS NOT NULL
                                    AND `tglProses` <= CURDATE();", con);
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
                            tglProses = Convert.ToDateTime(rd["tglProses"]),
                            source = rd["source"].ToString(),
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            bankid = Convert.ToInt32(rd["bankid"]),
                            id_billing_download = Convert.ToInt32(rd["id_billing_download"]),
                            deskripsi = rd["deskripsi"].ToString()
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message);
                Console.WriteLine("genFile " + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

            return Fileproses;
        }

        public static List<DataSubmitModel> PoolDataProsesApprove(string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            Console.Write("Pooling data Approve ...");
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=1 AND u.BillCode='B';", con);
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
                //throw new Exception(ex.Message);
                Console.WriteLine("PoolDataProses Recurring Approve =>" + ex.Message);
            }
            finally
            {
                con.Close();
            }

            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=1 AND u.BillCode='Q';", con);
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
                //throw new Exception(ex.Message);
                Console.WriteLine("PoolDataProses Quote Approve ERROR =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

            Console.WriteLine("Finish");
            return DataProses;
        }

        static List<DataRejectModel> PoolDataProsesReject(string tableName)
        {
            //IsSukses 0=Reject, 1=Approve
            Console.Write("Pooling data Reject ...");
            List<DataRejectModel> DataProses = new List<DataRejectModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=0 ;", con);
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
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            BillCode = rd["BillCode"].ToString(),
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message);
                Console.WriteLine("PoolDataProses Recurring Approve =>" + ex.Message);
            }
            finally
            {
                con.Close();
            }

            Console.WriteLine("Finish");
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
                //throw new Exception(ex.Message);
                Console.WriteLine("removeFile =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

        }

        //public static void InsertTableStagingAsync(List<DataUploadModel> DataUpload, string tableName)
        //{
        //    String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses) values ";
        //    string sql = "";
        //    int i = 0;
        //    List<Task> tasks = new List<Task>();

        //    foreach (DataUploadModel item in DataUpload)
        //    {
        //        i++;
        //        sql = sql + string.Format(@"('{0}',{1},'{2}','{3}','{4}','{5}',{6}),",
        //            item.PolisNo, item.Amount, item.ApprovalCode, item.Deskripsi, item.AccNo, item.AccName, item.IsSukses);

        //        tasks.Add(Task.Factory.StartNew(() =>
        //        {
        //            ExecQueryAsync(sqlStart + sql.TrimEnd(','));
        //        }));
        //        // eksekusi per 100 data
        //        //if (i == 100)
        //        //{
        //        //    ExecQueryAsync(sqlStart + sql.TrimEnd(',')).Wait();

        //        //    //Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
        //        //    sql = "";
        //        //    i = 0;
        //        //}
        //    }
        //    //eksekusi sisanya 
        //    //ExecQueryAsync(sqlStart + sql.TrimEnd(','));
        //    if (i > 0) Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
        //}

        public static void InsertTableStaging(List<DataUploadModel> DataUpload, string tableName)
        {
            Console.WriteLine();
            Console.Write("Insert into staging table ...");
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses) values ";
            string sql = "";
            int i = 0;
            foreach (DataUploadModel item in DataUpload)
            {
                if (item == null) continue;
                i++;
                sql = sql + string.Format(@"('{0}',{1},'{2}',NULLIF('{3}',''),'{4}','{5}',{6}),",
                    item.PolisNo, item.Amount, item.ApprovalCode, item.Deskripsi, item.AccNo, item.AccName, item.IsSukses);
                // eksekusi per 100 data
                if (i == 500)
                {
                    ExecQueryAsync(sqlStart + sql.TrimEnd(',')).Wait();
                    sql = "";
                    i = 0;
                }
            }
            //eksekusi sisanya 
            if (i > 0) ExecQueryAsync(sqlStart + sql.TrimEnd(',')).Wait();
            Console.WriteLine("Finish");
        }

        public static async Task ExecQueryAsync(string query)
        {
            using (MySqlConnection con = new MySqlConnection(constring))
            {
                MySqlCommand cmd = new MySqlCommand(query, con);
                cmd.Parameters.Clear();
                cmd.CommandType = CommandType.Text;
                try
                {
                    con.Open();
                    await cmd.ExecuteNonQueryAsync().ContinueWith(_ => con.CloseAsync());
                }
                catch (Exception ex)
                {
                    //throw new Exception(ex.Message);
                    Console.WriteLine("ExecQueryAsync =>" + ex.Message);
                }
            }
        }

        public static List<DataUploadModel> BacaFileBCA(FileResultModel Fileproses)
        {
            Console.Write("BacaFileBCA . . .");
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();
            using (StreamReader reader = new StreamReader(File.OpenRead(FileResult + Fileproses.FileName)))
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
                        IsSukses = (line.Substring(line.Length - 2) == "00") ? true : false
                    });
                }
            }
            Console.WriteLine("Finish");
            return dataUpload;
        }
        public static List<DataUploadModel> BacaFileMandiri(FileResultModel Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses.FileName, FileMode.Open))
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
                for (int row = startCell.Row; row <= endCell.Row; row++)
                {
                    if ((ws.Cells[row, 2].Value == null) || (ws.Cells[row, 3].Value == null) ||
                        (ws.Cells[row, 4].Value == null) || (ws.Cells[row, 5].Value == null) ||
                        (ws.Cells[row, 6].Value == null) || (ws.Cells[row, 7].Value == null)) continue;

                    if (!Decimal.TryParse(ws.Cells[row, 3].Value.ToString().Trim(), out tmp1)) continue;
                    dataUpload.Add(new DataUploadModel()
                    {
                        AccName = ws.Cells[row, 2].Value.ToString().Trim(),
                        Amount = tmp1,
                        PolisNo = ws.Cells[row, 4].Value.ToString().Trim(),
                        ApprovalCode = ws.Cells[row, 5].Value.ToString().Trim(),
                        Deskripsi = ws.Cells[row, 6].Value.ToString().Trim(),
                        AccNo = ws.Cells[row, 7].Value.ToString().Trim(),
                        IsSukses = false
                    });
                }
            }
            return dataUpload;
        }

        public static List<DataUploadModel> BacaFileMega(FileResultModel Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses.FileName, FileMode.Open))
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
        public static List<DataUploadModel> BacaFileBNI(FileResultModel Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            using (FileStream fs = new FileStream(FileResult + Fileproses.FileName, FileMode.Open))
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

        public static void KosongkanTabel()
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE FROM `UploadBcaCC`;
                                    DELETE FROM `UploadMandiriCC`;
                                    DELETE FROM `UploadMegaOnUsCC`;
                                    DELETE FROM `UploadMegaOfUsCC`;
                                    DELETE FROM `UploadBniCC`;", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message);
                Console.WriteLine("KosongkanTabel =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void MapingDataApprove(string tableName)
        {
            Console.Write("Mapping data approve ...");
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
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
	SELECT DISTINCT s.`PolisNo` FROM " + tableName + @" s WHERE s.`IsSukses`=1
) su ON su.`PolisNo`=pb.`policy_no`
WHERE b.status_billing IN ('A','C') 
ORDER BY b.policy_id,b.recurring_seq;
		
UPDATE " + tableName + @" up
SET up.`seqid`=CASE
	    WHEN @prev_value = up.`PolisNo` THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := up.`PolisNo` THEN @rank_count:=1
	END
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1) NOT IN ('A','X')
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
	up.`TotalAmount`=b.`TotalAmount`
WHERE up.`IsSukses`=1;

## isi data yang gak ada billing (karena akan create billing)
UPDATE " + tableName + @" up
INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
	SET up.`PolisId`=pb.`policy_Id`,
	up.`BillCode`='B',
	up.`PremiAmount`=pb.`regular_premium`,
	up.`CashlessFeeAmount`=pb.`cashless_fee_amount`,
	up.`TotalAmount`=pb.`regular_premium`+pb.`cashless_fee_amount`
WHERE up.`PolisId` IS NULL AND up.`IsSukses`=1
AND up.`IsSukses`=1
AND LEFT(up.`PolisNo`,1) NOT IN ('A','X');


UPDATE " + tableName + @" up
	SET up.`BillingID`=up.`PolisNo`,up.`BillCode`='A'
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1)='A';

UPDATE " + tableName + @" up
	set up.`BillingID`=TRIM(LEADING 'X' FROM up.`PolisNo`),
	up.`BillCode`='Q'
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1)='X';", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message);
                Console.WriteLine("MapingDataApprove =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
            Console.WriteLine("Finish");
        }

        public static void MapingDataReject(string tableName)
        {
            Console.Write("Mapping data Reject ...");
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
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
	SELECT DISTINCT s.`PolisNo` FROM " + tableName + @" s WHERE s.`IsSukses`=0
) su ON su.`PolisNo`=pb.`policy_no`
WHERE b.status_billing IN ('A','C') 
ORDER BY b.policy_id,b.recurring_seq;
		
UPDATE " + tableName + @" up
SET up.`seqid`=CASE
	    WHEN @prev_value = up.`PolisNo` THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := up.`PolisNo` THEN @rank_count:=1
	END
WHERE up.`IsSukses`=0 AND LEFT(up.`PolisNo`,1) NOT IN ('A','X')
ORDER BY up.`PolisNo`,up.`Amount`;

## Maping data upload yang ada billingnya
UPDATE " + tableName + @" up
INNER JOIN billx bx ON up.`PolisNo`=bx.`policy_no` AND up.`seqid`=bx.seqno
INNER JOIN `policy_billing` pb ON pb.`policy_Id`=bx.policy_id
INNER JOIN `billing` b ON b.`BillingID`=bx.BillingID
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode` AND rm.`bank_id`=1
SET up.`PolisId`=pb.`policy_Id`,	
	up.`BillingID`=b.`BillingID`,	
	up.`BillCode`='B',
	up.`Deskripsi`=COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`),NULLIF(up.`Deskripsi`,''))
WHERE up.`IsSukses`=0;

UPDATE " + tableName + @" up
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode` AND rm.`bank_id`=1
	SET up.`BillingID`=up.`PolisNo`,
	up.`BillCode`='A',
	up.`Deskripsi`=COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`),up.`Deskripsi`)
WHERE up.`IsSukses`=0 AND LEFT(up.`PolisNo`,1)='A';

UPDATE " + tableName + @" up
LEFT JOIN `reject_reason_map` rm ON rm.`reject_code`=up.`ApprovalCode` AND rm.`bank_id`=1
	SET up.`BillingID`=TRIM(LEADING 'X' FROM up.`PolisNo`),
	up.`BillCode`='Q',
	up.`Deskripsi`=COALESCE(CONCAT(rm.`reject_reason_bank`,' - ',rm.`reject_reason_caf`),up.`Deskripsi`)
WHERE up.`IsSukses`=0 AND LEFT(up.`PolisNo`,1)='X';

", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message);
                Console.WriteLine("MapingDataReject =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
            Console.WriteLine("Finish ...");
        }

        public static void SubmitApproveTransaction(string tableName, List<DataSubmitModel> DataProses, FileResultModel DataHeader)
        {
            Console.WriteLine("SubmitTransaction Begin ....");
            int i = 0;
            foreach (DataSubmitModel item in DataProses)
            {
                i++;
                try
                {
                    Console.Write(String.Format("{0} ", i));
                    //if (item.BillCode == "B") Task.Run(async() =>await RecurringApprove(tableName,item, DataHeader));
                    //if (item.BillCode == "B") RecurringApprove(tableName, item, DataHeader);
                    //else if (item.BillCode == "A") Task.Run(() => RecurringApprove());
                    //else 
                    if (item.BillCode == "Q") QuoteApprove(item, DataHeader).Wait();
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine("MySqlException SubmitTransaction ERROR =>" + ex.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(" Exception SubmitTransaction ERROR =>" + ex.Message);
                }

            }
            Console.WriteLine("SubmitTransaction Finish ....");
        }
        public static void RecurringApprove(string tableName, DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            // Fungsi Approve data Recurring, jadi harus ada polisID saat di mapping
            if ((DataProses.PolisId == null) || (DataProses.PolisId == "")) return;

            Console.Write(String.Format("Polis {0} ...", DataProses.PolisNo));

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
                    //DataProses.BillingID = cmd.ExecuteScalarAsync().Result.ToString();
                    DataProses.BillingID = cmd.ExecuteScalar().ToString();
                    Console.Write(String.Format("BillingID={0} ... ", DataProses.BillingID));
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
                Console.Write(String.Format("TransHistory={0} ... ", DataProses.TransHistory));

                // Insert Receipt
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt`(`receipt_date`,`receipt_policy_id`, `receipt_fund_type_id`, `receipt_transaction_code`, `receipt_amount`,
`receipt_source`, `receipt_status`, `receipt_payment_date_time`, `receipt_seq`, `bank_acc_id`, `due_date_pre`,`acquirer_bank_id`)
SELECT @tgl,up.`PolisId`,0,'RP',up.`Amount`-b.`cashless_fee_amount`,@source,'P',@tgl,b.`recurring_seq`,@bankAccId,b.`due_dt_pre`,@bankid
FROM " + tableName + @" up
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
                Console.Write(String.Format("receiptID={0} ... ", DataProses.receiptID));

                // Insert Receipt Other
                if (DataProses.CashlessFeeAmount > 0)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
SELECT @tgl,b.`policy_id`,3,b.`cashless_fee_amount`,@source,@tgl,b.`recurring_seq`,@bankAccId,@bankid
FROM " + tableName + @" up
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
                    Console.Write(String.Format("receiptOtherID={0} ... ", DataProses.receiptOtherID));
                }

                // Insert CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT INTO `prod_life21`.`policy_cc_transaction`(`policy_id`,`transaction_dt`,`transaction_type`,`recurring_seq`,
`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,`acquirer_bank_id`,
`cc_no`,`cc_name`,`status_id`,`remark`,`receipt_id`,`receipt_other_id`,`created_dt`)
SELECT up.`PolisId`,@tgl,'R',b.`recurring_seq`,1,'IDR',b.`TotalAmount`,b.`due_dt_pre`,DATE_FORMAT(b.`due_dt_pre`,'%b%d'),
@bankid,up.`AccNo`,up.`AccName`,2,'APPROVED',@receiptID,@receiptOtherID,@tgl
FROM " + tableName + @" up
INNER JOIN `billing` b ON b.`BillingID`=@BillID
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
                Console.Write(String.Format("TransID={0} ... ", DataProses.TransID));

                // Update Billing JBS
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"UPDATE `jbsdb`.`billing` SET `IsDownload`=0,
                                        `IsClosed`=1,
                                        `BillingDate`=COALESCE(`BillingDate`,@tgl),
                                        `status_billing`='P',
                                        `PaymentSource`='CC',
                                        `BankIdPaid`=@bankid,
                                        `PaidAmount`=@PaidAmount,
                                        `Life21TranID`=@TransactionID,
                                        `ReceiptID`=@receiptID,
                                        `ReceiptOtherID`=@ReceiptOtherID,
                                        `PaymentTransactionID`=@uid,
                                        `ACCname`=@ACCname,
                                        `ACCno`=@ACCno,
                                        `LastUploadDate`=@tgl,
                                        `UserUpload`='system'
                                    WHERE `BillingID`=@idBill;";
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

                // Insert EmailQuee
                var emailThanks = new EmailThanksRecuring(Convert.ToInt32(DataProses.BillingID), DataProses.Amount, DataHeader.tglSkrg);
                emailThanks.InsertEmailQuee();
                //Task.Run(async () => emailThanks.InsertEmailQuee()).Wait();
                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                Console.WriteLine("RecurringApprove ERROR =>" + ex.Message);
            }
            finally
            {
                con.Close();
            }

            Console.WriteLine("Finish");
        }

        public static async Task BillOtherApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.BillingID == null) || (DataProses.BillingID == "")) return;

            Console.Write(String.Format("Polis {0} ...", DataProses.PolisNo));

            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                Console.WriteLine("BillOtherApprove Error =>" + ex.Message);
            }
            finally
            {
                Task.WaitAll();
                await con.CloseAsync();
            }
        }

        public static async Task QuoteApprove(DataSubmitModel DataProses, FileResultModel DataHeader)
        {
            if ((DataProses.BillingID == null) || (DataProses.BillingID == "")) return;
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();
            Console.Write(String.Format("Quote {0} ...", DataProses.BillingID));
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
			                                    `PaymentTransactionID`=@uid,
                                                UserUpload='system'
		                                    WHERE `quote_id`=@quoteID;";
                cmd.Parameters.Add(new MySqlParameter("@quoteID", MySqlDbType.Int32) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@PaidAmount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.ExecuteNonQueryAsync().Wait();

                // Insert EmailQuee
                var emailQuoteThanks = new EmailThanksQuote(Convert.ToInt32(DataProses.BillingID), DataProses.Amount, DataHeader.tglSkrg);
                //emailThanks.InsertEmailQuee();
                emailQuoteThanks.InsertEmailQuee();
                //Task.Run(async () => emailThanks.InsertEmailQuee()).Wait();

                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                Console.WriteLine("QuoteApprove Error =>" + ex.Message);
            }
            finally
            {
                Task.WaitAll();
                await con.CloseAsync();
            }
        }

        public static void SubmitRejectTransaction(string tableName, List<DataRejectModel> DataProses, FileResultModel DataHeader)
        {
            Console.WriteLine("RejectTransaction Begin ....");
            int i = 0;
            foreach (DataRejectModel item in DataProses)
            {
                i++;
                try
                {
                    Console.Write(String.Format("{0} ", i));
                    RejectTransaction(item, DataHeader).Wait();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("RejectTransaction =>" + ex.Message);
                }
            }
            Task.WaitAll();
            Console.WriteLine("RejectTransaction Finish ....");
        }

        public static async Task RejectTransaction(DataRejectModel DataProses, FileResultModel DataHeader)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlTransaction tr = null;
            MySqlCommand cmd = new MySqlCommand();

            try
            {
                con.Open();
                tr = con.BeginTransaction();
                cmd.Connection = con;
                cmd.Transaction = tr;

                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `jbsdb`.`transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
SELECT @FileBackup,@TranCode,b.`policy_id`,b.`BillingID`,@BillAmount,@ApprovalCode,@Description,
    COALESCE(IFNULL(@accNo,''),b.`AccNo`,pc.`cc_no`),COALESCE(IFNULL(@accName,''),b.`AccName`,pc.`cc_name`)
FROM `billing` b
INNER JOIN `policy_billing` pb ON pb.`policy_Id`=b.`policy_id`
INNER JOIN `policy_cc` pc ON pc.`PolicyId`=pb.`policy_Id`
WHERE pb.`policy_no`=@polisno
AND b.`status_billing` IN ('A','C')
ORDER BY b.`recurring_seq` ASC
LIMIT 1;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.VarChar) { Value = DataHeader.FileSaveName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.VarChar) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.VarChar) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.VarChar) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.VarChar) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.VarChar) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.VarChar) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalar().ToString();
                Console.Write(String.Format("TransHistory={0} ... ", DataProses.TransHistory));

                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                if (DataProses.BillCode == "B")
                {
                    // Update Billing JBS                    
                    cmd.CommandText = @"UPDATE `billing` bb
                                        INNER JOIN (
	                                        SELECT b.`BillingID`
	                                        FROM `billing` b
	                                        INNER JOIN `policy_billing` pb ON pb.`policy_Id`=b.`policy_id`
	                                        WHERE b.`status_billing` IN ('A','C') AND pb.`policy_no`=@polisno
	                                        ORDER BY b.`recurring_seq` ASC
	                                        LIMIT 1
                                        )bx ON bb.`BillingID`=bx.`BillingID`
                                        SET bb.`IsDownload`=0,
                                        bb.`PaymentTransactionID`=@uid,
                                        bb.`LastUploadDate`=@tgl,
                                        bb.`UserUpload`='system'
                                    WHERE bb.`status_billing` IN ('A','C');";
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                    cmd.Parameters.Add(new MySqlParameter("@polisno", MySqlDbType.Int32) { Value = DataProses.PolisNo });
                }
                else if (DataProses.BillCode == "A")
                {
                    cmd.CommandText = @"UPDATE `billing_others` 
                                        SET `IsDownload`=0,
                                        `BillingDate`=COALESCE(`BillingDate`,@tgl),
                                        `PaymentTransactionID`=@uid,
                                        `LastUploadDate`=@tgl,
                                        `UserUpload`='system' 
                                        WHERE `quote_id`=@billid AND `status_billing` in ('A','C')";
                }
                else if (DataProses.BillCode == "Q")
                {
                    cmd.CommandText = @"UPDATE `quote_billing` 
                                        SET `IsDownload`=0,
                                        `BillingDate`=COALESCE(`BillingDate`,@tgl),
                                        `PaymentTransactionID`=@uid,
                                        `LastUploadDate`=@tgl,
                                        `UserUpload`='system' 
                                        WHERE `quote_id`=@billid AND `status` in ('A','C')";
                }
                cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@uid", MySqlDbType.Int32) { Value = DataProses.TransHistory });
                cmd.Parameters.Add(new MySqlParameter("@idBill", MySqlDbType.Int32) { Value = DataProses.PolisNo.Substring(1) });
                cmd.ExecuteNonQuery();
                tr.Commit();
            }
            catch (Exception ex)
            {
                tr.Rollback();
                Console.WriteLine("InsertHistoryTransaction =>" + ex.Message);
            }
            finally
            {
                Task.WaitAll();
                await con.CloseAsync();
            }

        }

    }
}
