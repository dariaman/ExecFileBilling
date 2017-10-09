using MySql.Data.MySqlClient;
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

        static void Main(string[] args)
        {
            var Fileproses = genFile();
            List<DataUploadModel> DataUpload;
            foreach (FileResultModel item in Fileproses)
            {
                DataUpload = new List<DataUploadModel>();

                Console.WriteLine(item.FileName);
                switch (item.Id)
                {
                    case 1:
                    case 2:
                        DataUpload = BacaFileBCA(item);
                        break;
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                        break;
                }
                removeFile(item);
            }

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

        public static void removeFile(FileResultModel Fileproses)
        {
            MySqlConnection con = new MySqlConnection(constring);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"UPDATE `FileNextProcess` SET `FileName`=NULL,`tglProses`=NULL
                                    WHERE `id`=@id;", con);
            cmd.CommandType = CommandType.Text;
            cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = Fileproses.Id });
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
                FileInfo Filex = new FileInfo(FileResult + Fileproses.FileName);
                if (Filex.Exists) Filex.MoveTo(FileBackup + Fileproses.FileName);
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

        public static List<DataUploadModel> BacaFileBCA(FileResultModel Fileproses)
        {
            List<DataUploadModel> dataUpload = new List<DataUploadModel>();

            //FileInfo Filex = new FileInfo(FileResult + Fileproses.FileName);
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
                        ApprovalCode= line.Substring(line.Length - 8).Substring(0, 6),
                        Deskripsi = line.Substring(line.Length - 2)
                    });
                }
            }
            return dataUpload;
        }
        public static void BacaFileMandiri()
        {

        }
        public static void BacaFileMega()
        {

        }
        public static void BacaFileBNI()
        {

        }
    }
}
