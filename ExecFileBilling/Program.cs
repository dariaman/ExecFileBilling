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
        //static DateTime tglSekarang = DateTime.Now;

        static string BCA_TEMP_TABLE = "UploadBcaCC";
        static string MANDIRI_TEMP_TABLE = "UploadMandiriCC";

        static void Main(string[] args)
        {
            var Fileproses = genFile();
            List<DataUploadModel> DataUpload;
            List<DataSubmitModel> DataProses;
            KosongkanTabel();
            DateTime tglSekarang = DateTime.Now;

            foreach (FileResultModel item in Fileproses)
            {
                item.FileSaveName = item.FileName + Guid.NewGuid().ToString().Substring(0, 8);
                item.tglSkrg = DateTime.Now;
                DataUpload = new List<DataUploadModel>();
                Console.WriteLine(item.FileName);
                switch (item.Id)
                {
                    case 1: // BCA Approve
                        DataUpload = BacaFileBCA(item);
                        InsertTableStaging(DataUpload, BCA_TEMP_TABLE);
                        MapingDataApprove(BCA_TEMP_TABLE);
                        DataProses = PoolDataProsesApprove(BCA_TEMP_TABLE);
                        SubmitTransaction(BCA_TEMP_TABLE, DataProses, item);
                        break;
                    case 2: // BCA Reject
                        //DataUpload = BacaFileBCA(item);
                        //InsertTableStaging(DataUpload, BCA_TEMP_TABLE);
                        //MapingDataReject(BCA_TEMP_TABLE);
                        //DataProses = PoolDataProsesApprove(BCA_TEMP_TABLE);
                        break;
                    case 3: // Mandiri
                            //DataUpload = BacaFileMandiri(item);
                            //InsertTableStagingAsync(DataUpload, MANDIRI_TEMP_TABLE);
                            //MapingDataApprove(MANDIRI_TEMP_TABLE);
                            //DataProses = PoolDataProsesApprove(MANDIRI_TEMP_TABLE);
                            //break;
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
            Console.Write("Pooling data proses ...");
            List<DataSubmitModel> DataProses = new List<DataSubmitModel>();
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM " + tableName + " u WHERE u.`IsSukses`=1 AND u.BillCode='B';", con);
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
                            seqid = Convert.ToInt32(rd["seqid"]),
                            PolisNo = rd["PolisNo"].ToString(),
                            Amount = Convert.ToDecimal(rd["Amount"]),
                            ApprovalCode = rd["ApprovalCode"].ToString(),
                            Deskripsi = rd["Deskripsi"].ToString(),
                            AccNo = rd["AccNo"].ToString(),
                            AccName = rd["AccName"].ToString(),
                            IsSukses = Convert.ToBoolean(rd["IsSukses"]),
                            PolisId = rd["PolisId"].ToString(),
                            BillingID = (rd["BillingID"] == DBNull.Value) ? null : rd["BillingID"].ToString(),
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
                Console.WriteLine("PoolDataProsesApprove =>" + ex.Message);
            }
            finally
            {
                con.CloseAsync();
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

        public static void InsertTableStagingAsync(List<DataUploadModel> DataUpload, string tableName)
        {
            String sqlStart = @"INSERT INTO " + tableName + "(PolisNo,Amount,ApprovalCode,Deskripsi,AccNo,AccName,IsSukses) values ";
            string sql = "";
            int i = 0;
            List<Task> tasks = new List<Task>();

            foreach (DataUploadModel item in DataUpload)
            {
                i++;
                sql = sql + string.Format(@"('{0}',{1},'{2}','{3}','{4}','{5}',{6}),",
                    item.PolisNo, item.Amount, item.ApprovalCode, item.Deskripsi, item.AccNo, item.AccName, item.IsSukses);

                tasks.Add(Task.Factory.StartNew(() =>
                {
                    ExecQueryAsync(sqlStart + sql.TrimEnd(','));
                }));
                // eksekusi per 100 data
                //if (i == 100)
                //{
                //    ExecQueryAsync(sqlStart + sql.TrimEnd(',')).Wait();

                //    //Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
                //    sql = "";
                //    i = 0;
                //}
            }
            //eksekusi sisanya 
            //ExecQueryAsync(sqlStart + sql.TrimEnd(','));
            if (i > 0) Task.Run(() => ExecQueryAsync(sqlStart + sql.TrimEnd(',')));
        }

        public static void InsertTableStaging(List<DataUploadModel> DataUpload, string tableName)
        {
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
	SELECT DISTINCT s.`PolisNo` FROM UploadBcaCC s WHERE s.`IsSukses`=1
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
	SELECT DISTINCT s.`PolisNo` FROM UploadBcaCC s WHERE s.`IsSukses`=0
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
        }

        public static void SubmitTransaction(string tableName, List<DataSubmitModel> DataProses, FileResultModel DataHeader)
        {
            Console.WriteLine("SubmitTransaction Begin ....");
            //try
            //{
            int i = 0;
            foreach (DataSubmitModel item in DataProses)
            {
                i++;
                try
                {
                    Console.Write(String.Format("{0} ",i));
                    //if (item.BillCode == "B") Task.Run(async() =>await RecurringApprove(tableName,item, DataHeader));
                    if (item.BillCode == "B") RecurringApprove(tableName, item, DataHeader).Wait();
                    //if (item.BillCode == "A") Task.Run(() => RecurringApprove());
                    //if (item.BillCode == "Q") Task.Run(() => RecurringApprove());
                }
                catch (Exception ex)
                {
                    Console.WriteLine("SubmitTransaction =>" + ex.Message);
                }
            }
            Console.WriteLine("SubmitTransaction Finsih ....");
        }
        public static async Task RecurringApprove(string tableName, DataSubmitModel DataProses, FileResultModel DataHeader)
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
                    cmd.Parameters.Add(new MySqlParameter("@polisId", MySqlDbType.String) { Value = DataProses.PolisId });
                    DataProses.BillingID = cmd.ExecuteScalarAsync().Result.ToString();
                    Console.Write(String.Format("BillingID={0} ...", DataProses.BillingID));
                }

                // Create History Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"
INSERT `transaction_bank`(`File_Backup`,`TranCode`,`PolicyId`,`BillingID`,`BillAmount`,`ApprovalCode`,`Description`,`accNo`,`accName`)
values (@FileBackup,@TranCode,@PolicyId,@BillingID,@BillAmount,@ApprovalCode,@Description,@accNo,@accName);
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@FileBackup", MySqlDbType.String) { Value = DataHeader.FileSaveName });
                cmd.Parameters.Add(new MySqlParameter("@TranCode", MySqlDbType.String) { Value = DataHeader.trancode });
                cmd.Parameters.Add(new MySqlParameter("@PolicyId", MySqlDbType.String) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@BillingID", MySqlDbType.String) { Value = DataProses.BillingID });
                cmd.Parameters.Add(new MySqlParameter("@BillAmount", MySqlDbType.String) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@ApprovalCode", MySqlDbType.String) { Value = DataProses.ApprovalCode });
                cmd.Parameters.Add(new MySqlParameter("@Description", MySqlDbType.String) { Value = DataProses.Deskripsi });
                cmd.Parameters.Add(new MySqlParameter("@accNo", MySqlDbType.String) { Value = DataProses.AccNo });
                cmd.Parameters.Add(new MySqlParameter("@accName", MySqlDbType.String) { Value = DataProses.AccName });
                DataProses.TransHistory = cmd.ExecuteScalarAsync().Result.ToString();
                Console.Write(String.Format("TransHistory={0} ...", DataProses.TransHistory));

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
                cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.id_billing_download });
                DataProses.receiptID = cmd.ExecuteScalarAsync().Result.ToString();
                Console.Write(String.Format("receiptID={0} ... ", DataProses.receiptID));

                // Insert Receipt Other
                if (DataProses.CashlessFeeAmount > 0)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.CommandText = @"
INSERT INTO `prod_life21`.`receipt_other`(`receipt_date`,`policy_id`,`receipt_type_id`,`receipt_amount`,`receipt_source`,`receipt_payment_date`,`receipt_seq`,`bank_acc_id`,`acquirer_bank_id`)
SELECT @tgl,b.`policy_id`,3,b.`cashless_fee_amount`,@source,@tgl,b.`recurring_seq`,@bankAccId,@bankid
FROM `UploadBcaCC` up
LEFT JOIN `billing` b ON b.`BillingID`=@Billid
WHERE up.`seqid`=@SeqId AND up.`PolisNo`=@PolisNo;
SELECT LAST_INSERT_ID();";
                    cmd.Parameters.Add(new MySqlParameter("@SeqId", MySqlDbType.Int32) { Value = DataProses.seqid });
                    cmd.Parameters.Add(new MySqlParameter("@PolisNo", MySqlDbType.VarChar) { Value = DataProses.PolisNo });
                    cmd.Parameters.Add(new MySqlParameter("@Billid", MySqlDbType.Int32) { Value = DataProses.BillingID });
                    cmd.Parameters.Add(new MySqlParameter("@tgl", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                    cmd.Parameters.Add(new MySqlParameter("@source", MySqlDbType.VarChar) { Value = DataHeader.source });
                    cmd.Parameters.Add(new MySqlParameter("@bankAccId", MySqlDbType.Int32) { Value = DataHeader.bankid_receipt });
                    cmd.Parameters.Add(new MySqlParameter("@bankid", MySqlDbType.Int32) { Value = DataHeader.id_billing_download });
                    DataProses.receiptOtherID = cmd.ExecuteScalarAsync().Result.ToString();
                    Console.Write(String.Format("receiptOtherID={0} ... ", DataProses.receiptOtherID));
                }


                // Insert CC Transaction
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Clear();
                cmd.CommandText = @"INSERT INTO `prod_life21`.`policy_cc_transaction`(`policy_id`,`transaction_dt`,`transaction_type`,
`recurring_seq`,`count_times`,`currency`,`total_amount`,`due_date_pre`,`due_date_pre_period`,
`cycle_date`,`acquirer_bank_id`,`status_id`,`remark`,`receipt_id`,`receipt_other_id`,`created_dt`,
`cc_no`,`cc_name`,`cc_expiry`,`update_dt`)
SELECT @PolisID,@Transdate,@billType,@Seq,1,'IDR',@Amount,@DueDatePre,@Period,@CycleDate,@BankID,2,'APPROVED',@receiptID,NULLIF(@receiptOtherID,0),@Transdate,
@CCno, @CCName, @CCExpiry,@Transdate;
SELECT LAST_INSERT_ID();";
                cmd.Parameters.Add(new MySqlParameter("@PolisID", MySqlDbType.Int32) { Value = DataProses.PolisId });
                cmd.Parameters.Add(new MySqlParameter("@Transdate", MySqlDbType.DateTime) { Value = DataHeader.tglSkrg });
                cmd.Parameters.Add(new MySqlParameter("@billType", MySqlDbType.VarChar) { Value = "R" });
                cmd.Parameters.Add(new MySqlParameter("@Seq", MySqlDbType.Int32) { Value = DataProses.seqid });
                cmd.Parameters.Add(new MySqlParameter("@Amount", MySqlDbType.Decimal) { Value = DataProses.Amount });
                cmd.Parameters.Add(new MySqlParameter("@DueDatePre", MySqlDbType.Date) { Value = pt.Due_Date_Pre });
                cmd.Parameters.Add(new MySqlParameter("@Period", MySqlDbType.VarChar) { Value = String.Format("{0:MMMdd}", pt.Due_Date_Pre) });
                cmd.Parameters.Add(new MySqlParameter("@CycleDate", MySqlDbType.Int32) { Value = String.Format("{0:dd}", pt.Due_Date_Pre) });
                cmd.Parameters.Add(new MySqlParameter("@BankID", MySqlDbType.Int32) { Value = pt.BankID });
                cmd.Parameters.Add(new MySqlParameter("@receiptID", MySqlDbType.Int32) { Value = pt.receipt_id });
                cmd.Parameters.Add(new MySqlParameter("@receiptOtherID", MySqlDbType.Int32) { Value = pt.receipt_other_id });

                cmd.Parameters.Add(new MySqlParameter("@CCno", MySqlDbType.VarChar) { Value = pt.ACC_No });
                cmd.Parameters.Add(new MySqlParameter("@CCName", MySqlDbType.VarChar) { Value = pt.ACC_Name });
                cmd.Parameters.Add(new MySqlParameter("@CCExpiry", MySqlDbType.VarChar) { Value = pt.CC_expiry });
                Console.Write(String.Format("receiptID={0} ... ", DataProses.receiptID));

                tr.Rollback();
            }
            catch (MySqlException ex)
            {
                tr.Rollback();
                Console.WriteLine("RecurringApprove =>" + ex.Message);
            }

            Console.WriteLine("Finish");
        }

    }
}
