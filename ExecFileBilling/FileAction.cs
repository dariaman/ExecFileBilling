using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class FileAction
    {
        static string FileResult = ConfigurationManager.AppSettings["DirResult"];

        public FileAction()
        {

        }

        public DataUploadModel BacaLineTextBCA(String LineText)
        {
            DataUploadModel dataUpload = new DataUploadModel();
            Decimal tmp1;

            if (LineText.Length < 171) return dataUpload;
            if (!Decimal.TryParse(LineText.Substring(54, 9), out tmp1)) return null;
            dataUpload.PolisNo = LineText.Substring(9, 25).Trim();
            dataUpload.AccNo = LineText.Substring(34, 16).Trim();
            dataUpload.AccName = LineText.Substring(65, 26).Trim();
            dataUpload.Amount = tmp1;
            dataUpload.ApprovalCode = (LineText.Substring(LineText.Length - 2) == "00")
                                    ? LineText.Substring(LineText.Length - 8).Substring(0, 6).Trim()
                                    : LineText.Substring(LineText.Length - 2);
            dataUpload.Deskripsi = null;
            dataUpload.IsSukses = (LineText.Substring(LineText.Length - 2) == "00") ? true : false;
            return dataUpload;
        }

        // Baca File Approve dan reject
        public List<DataUploadModel> BacaFileBCA(String Fileproses)
        {
            if(!File.Exists(FileResult + Fileproses)) return new List<DataUploadModel>();
            List<Task> tasks = new List<Task>();
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
                    tasks.Add(Task.Factory.StartNew( () =>
                    {
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
                    }));

                }

            }
            Task.Factory.StartNew(() => {
                Task.WaitAll(tasks.ToArray());
                Console.WriteLine("Finished");
            });
            return dataUpload;
        }
    }
}
