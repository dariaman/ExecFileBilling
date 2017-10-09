using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class FileResultModel
    {
        public int Id { get; set; }
        public string trancode { get; set; }
        public string FileName { get; set; }
        public DateTime tglProses { get; set; }
        public int bankid_receipt { get; set; }
        public string deskripsi { get; set; }
    }
}
