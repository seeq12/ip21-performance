using System;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics;

namespace IP21Performance {

    internal class OdbcMethod : Method {

        public string Go(string hostname, string port, string username, string password, string tag, DateTime startTime, DateTime endTime) {
            string output = "";

            if (string.IsNullOrWhiteSpace(username)) {
                username = null;
            }

            if (string.IsNullOrWhiteSpace(password)) {
                password = null;
            }

            string connStr = "DRIVER={AspenTech SQLplus};HOST=" + hostname + ";PORT=" + port + ";";
            OdbcConnection conn = new OdbcConnection(connStr);

            const string IP21TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

            string sqlPlusText = string.Format("SELECT ISO8601(IP_TREND_TIME), IP_TREND_VALUE " +
                    "FROM \"{0}\" WHERE IP_TREND_TIME BETWEEN '{1}' AND '{2}'",
                    tag,
                    startTime.ToUniversalTime().ToString(IP21TimestampFormat),
                    endTime.ToUniversalTime().ToString(IP21TimestampFormat));

            output += "SQLplus:\n\n" + sqlPlusText + "\n\n";

            OdbcDataAdapter adapter = new OdbcDataAdapter(sqlPlusText, conn);

            conn.Open();

            var ds = new DataSet();

            for (int i = 0; i < 2; i++) {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                int recordsReturned = adapter.Fill(ds);
                stopwatch.Stop();

                output += string.Format("Query {3}: IP21 data page returned {0} samples in {1} ms, which is {2} samples per second\n",
                    recordsReturned,
                    stopwatch.Elapsed.TotalMilliseconds,
                    ((double)recordsReturned) / stopwatch.Elapsed.TotalMilliseconds * 1000d,
                    i + 1);
            }

            conn.Close();

            return output;
        }
    }
}