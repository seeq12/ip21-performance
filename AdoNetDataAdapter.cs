using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IP21Performance {

    internal class AdoNetDataAdapter : Method {

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

                using (DbDataAdapter adapter = new AspenTech.SQLplus.SQLplusDataAdapter(sqlPlusText, sqlPlusConnection)) {
                    using (DataSet dataSet = new DataSet()) {
                        for (int i = 0; i < 2; i++) {
                            Stopwatch stopwatch = new Stopwatch();
                            stopwatch.Start();
                            int recordsReturned = adapter.Fill(dataSet, 0, 1000, "Table");
                            stopwatch.Stop();

                            output += string.Format("Query {3}: IP21 data page returned {0} samples in {1} ms, which is {2} samples per second\n",
                                recordsReturned,
                                stopwatch.Elapsed.TotalMilliseconds,
                                ((double)recordsReturned) / stopwatch.Elapsed.TotalMilliseconds * 1000d,
                                i + 1);
                        }
                    }
                }
            }

            return output;
        }
    }
}