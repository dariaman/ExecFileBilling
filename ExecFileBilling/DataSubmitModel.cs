﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExecFileBilling
{
    class DataSubmitModel
    {
        public string PolisNo { get; set; }
        public Decimal Amount { get; set; }
        public string ApprovalCode { get; set; }
        public string Deskripsi { get; set; }
        public string AccNo { get; set; }
        public string AccName { get; set; }
        public Boolean IsSukses { get; set; }
        public string PolisId { get; set; }
        public string BillingID { get; set; }
        public string BillCode { get; set; }
        public string BillStatus { get; set; }
        public string PolisStatus { get; set; }
    }
}