using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class ErrorLog
    {
        public string trancode { get; set; }
        public string PolisNo { get; set; }
        public Boolean IsSukses { get; set; }
        public string ErrorMessage { get; set; }

        public ErrorLog()
        {

        }

    }
}
