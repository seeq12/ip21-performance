using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace IP21Performance {

    internal class AtTagBrowserMethod : Method {

        public string Go(string hostname, string port, string username, string password, string tag, DateTime startTime, DateTime endTime) {
            string output = "";

            if (string.IsNullOrWhiteSpace(username)) {
                username = null;
            }

            if (string.IsNullOrWhiteSpace(password)) {
                password = null;
            }

            var connectionStringBuilder = new AspenTech.SQLplus.SQLplusConnectionStringBuilder();
            connectionStringBuilder.Host = hostname;
            connectionStringBuilder.Port = port;
            connectionStringBuilder.Uid = username;
            connectionStringBuilder.Pwd = password;
            connectionStringBuilder.ReadOnly = true;

            AtTagBrowserSQLplus.TagBrowser ip21Connection = new AtTagBrowserSQLplus.TagBrowser();

            try {
                ip21Connection.Connect("MyName", hostname, port, username, password);

                const string IP21TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

                using (AspenTech.SQLplus.SQLplusConnection sqlPlusConnection =
                    new AspenTech.SQLplus.SQLplusConnection(connectionStringBuilder.ConnectionString)) {
                    sqlPlusConnection.Open();

                    string sqlPlusText = string.Format("SELECT ISO8601(IP_TREND_TIME), IP_TREND_VALUE " +
                        "FROM \"{0}\" WHERE IP_TREND_TIME BETWEEN '{1}' AND '{2}'",
                        tag,
                        startTime.ToUniversalTime().ToString(IP21TimestampFormat),
                        endTime.ToUniversalTime().ToString(IP21TimestampFormat));

                    output += "SQLplus:\n\n" + sqlPlusText + "\n\n";

                    for (int i = 0; i < 2; i++) {
                        Stopwatch stopwatch = new Stopwatch();
                        stopwatch.Start();
                        ADODB.Recordset rs = ip21Connection.Query(sqlPlusText);
                        System.Data.OleDb.OleDbDataAdapter da = new System.Data.OleDb.OleDbDataAdapter();
                        DataSet ds = new DataSet();
                        da.Fill(ds, rs, "Tags");
                        DataTable dt = ds.Tables[0];
                        stopwatch.Stop();

                        output += string.Format("Query {3}: IP21 data page returned {0} samples in {1} ms, which is {2} samples per second\n",
                            dt.Rows.Count,
                            stopwatch.Elapsed.TotalMilliseconds,
                            ((double)dt.Rows.Count) / stopwatch.Elapsed.TotalMilliseconds * 1000d,
                            i + 1);
                    }
                }
            } finally {
                ip21Connection.Disconnect();
            }

            return output;
        }
    }
}