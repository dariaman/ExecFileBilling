using MySql.Data.MySqlClient;
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
        static DateTime tglSekarang = DateTime.Now;

        static string BCA_TEMP_TABLE = "UploadBcaCC";
        static string MANDIRI_TEMP_TABLE = "UploadMandiriCC";

        static void Main(string[] args)
        {
            var Fileproses = genFile();
            List<DataUploadModel> DataUpload;
            List<DataSubmitModel> DataProses;
            KosongkanTabel();
            foreach (FileResultModel item in Fileproses)
            {
                DataUpload = new List<DataUploadModel>();
                Console.WriteLine(item.FileName);
                switch (item.Id)
                {
                    case 1: // BCA Approve
                    case 2: // BCA Reject
                        DataUpload = BacaFileBCA(item);
                        InsertTableStagingAsync(DataUpload, BCA_TEMP_TABLE);
                        MapingDataApprove(BCA_TEMP_TABLE);
                        DataProses = PoolDataProsesApprove(BCA_TEMP_TABLE);
                        break;
                    case 3: // Mandiri
                        DataUpload = BacaFileMandiri(item);
                        InsertTableStagingAsync(DataUpload, MANDIRI_TEMP_TABLE);
                        MapingDataApprove(MANDIRI_TEMP_TABLE);
                        DataProses = PoolDataProsesApprove(MANDIRI_TEMP_TABLE);
                        break;
                    case 4:
                    case 5:
                    case 6:
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
            cmd = new MySqlCommand(@"SELECT `id`,`trancode`,`FileName`,`tglProses`,`bankid_receipt`,`deskripsi` 
                                    FROM `FileNextProcess`
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
                            bankid_receipt = Convert.ToInt32(rd["bankid_receipt"]),
                            deskripsi = rd["deskripsi"].ToString()
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

            return Fileproses;
        }

        public static List<DataSubmitModel> PoolDataProsesApprove(string tableName)
        {
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=1;", con);
            cmd.CommandType = CommandType.Text;
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
                            PolisId = rd["PolisId"].ToString(),
                            BillingID = rd["BillingID"].ToString(),
                            BillCode = rd["BillCode"].ToString(),
                            BillStatus = rd["BillStatus"].ToString(),
                            PolisStatus = rd["PolisStatus"].ToString()
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
            return DataProses;
        }

        public static void removeFile(FileResultModel Fileproses)
        {
            var idFile = Guid.NewGuid().ToString().Substring(0, 8);
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
                if (Filex.Exists) Filex.MoveTo(FileBackup + Fileproses.FileName + idFile);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }

        }

        public static void InsertTableStagingAsync(List<DataUploadModel> DataUpload, string tableName)
        {
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses) values ";
            string sql = "";
            int i = 0;
            foreach (DataUploadModel item in DataUpload)
            {
                i++;
                sql = sql + string.Format(@"('{0}',{1},'{2}','{3}','{4}','{5}',{6}),",
                    item.PolisNo, item.Amount, item.ApprovalCode, item.Deskripsi, item.AccNo, item.AccName, item.IsSukses);
                // eksekusi per 100 data
                if (i == 1000)
                {
                    //ExecQueryAsync(sqlStart + sql.TrimEnd(','));
                    Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
                    sql = "";
                    i = 0;
                }
            }
            //eksekusi sisanya 
            //ExecQueryAsync(sqlStart + sql.TrimEnd(','));
            if (i > 0) Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
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
                    await cmd.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }
            }
        }

        public static List<DataUploadModel> BacaFileBCA(FileResultModel Fileproses)
        {
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
                        AccName = ws.Cells[row, 2].Value.ToString(),
                        Amount = tmp1,
                        ApprovalCode = ws.Cells[row, 4].Value.ToString(),
                        PolisNo = ws.Cells[row, 6].Value.ToString(),
                        AccNo = ws.Cells[row, 7].Value.ToString(),
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
                        AccName = ws.Cells[row, 2].Value.ToString(),
                        Amount = tmp1,
                        PolisNo = ws.Cells[row, 4].Value.ToString(),
                        ApprovalCode = ws.Cells[row, 5].Value.ToString(),
                        Deskripsi = ws.Cells[row, 6].Value.ToString(),
                        AccNo = ws.Cells[row, 7].Value.ToString(),
                        IsSukses = false
                    });
                }

            }

            return dataUpload;
        }
        public static void BacaFileMega()
        {

        }
        public static void BacaFileBNI()
        {

        }

        public static void KosongkanTabel()
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"DELETE FROM `UploadBcaCC`;DELETE FROM UploadMandiriCC;", con);
            cmd.Parameters.Clear();
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void MapingDataApprove(string tableName)
        {
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
INNER JOIN " + tableName + @" su ON su.`PolisNo`=pb.`policy_no`
WHERE b.status_billing IN ('A','C') and su.`IsSukses`=1
ORDER BY b.policy_id,b.recurring_seq;
		
UPDATE " + tableName + @" up
SET up.`seqid`=CASE
	    WHEN @prev_value = up.`PolisNo` THEN @rank_count := @rank_count + 1
	    WHEN @prev_value := up.`PolisNo` THEN @rank_count:=1
	END
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1) NOT IN ('A','X')
ORDER BY up.`PolisNo`,up.`Amount`;

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

UPDATE " + tableName + @" up
INNER JOIN `policy_billing` pb ON pb.`policy_no`=up.`PolisNo`
	SET up.`PolisId`=pb.`policy_Id`,
	up.`BillCode`='B',
	up.`PremiAmount`=pb.`regular_premium`,
	up.`CashlessFeeAmount`=pb.`cashless_fee_amount`,
	up.`TotalAmount`=pb.`regular_premium`+pb.`cashless_fee_amount`
WHERE up.`PolisId` IS NULL
AND up.`IsSukses`=1
AND LEFT(up.`PolisNo`,1) NOT IN ('A','X');


UPDATE " + tableName + @" up
	SET up.`BillingID`=up.`PolisNo`,up.`BillCode`='A'
WHERE up.`IsSukses`=1 AND LEFT(up.`PolisNo`,1)='A';

UPDATE " + tableName + @" up
	set up.`BillingID`=TRIM(LEADING 'X' FROM up.`PolisNo`),
	up.`BillCode`='X'
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
                throw new Exception(ex.Message);
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static void SubmitTransaction(List<DataSubmitModel> DataProses)
        {
            try
            {
                foreach (DataSubmitModel item in DataProses)
                {
                    if (item.BillCode == "R") Task.Run(() => RecurringApprove(item));
                    //if (item.BillCode == "A") Task.Run(() => RecurringApprove());
                    //if (item.BillCode == "Q") Task.Run(() => RecurringApprove());
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        public static void RecurringApprove(DataSubmitModel DataProses)
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

                var billID = (DataProses.BillingID == "") ? DataProses.BillingID : CreateNewBilling(ref cmd, DataProses.PolisId);
                tr.Rollback();
            }
            catch (MySqlException ex)
            {
                tr.Rollback();
            }
            finally
            {
                con.CloseAsync();
            }
        }

        public static string CreateNewBilling(ref MySqlCommand cmd, string polisID)
        {
            string hasil = "";
            //MySqlConnection con = new MySqlConnection(constring);
            //MySqlCommand cmd;
            //cmd = new MySqlCommand(@"CreateNewBillingRecurring", con);
            //cmd.Transaction = tr;

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText= "CreateNewBillingRecurring";
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@polisId", MySqlDbType.Int32) { Value = polisID });
            try
            {
                //con.Open();
                hasil = cmd.ExecuteScalar().ToString();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            //finally
            //{
            //    con.CloseAsync();
            //}
            return hasil;
        }
    }
}
