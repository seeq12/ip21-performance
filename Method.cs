using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IP21Performance {

    internal interface Method {

        string Go(string hostname, string port, string username, string password, string tag, DateTime startTime, DateTime endTime);
    }
}