using System;
using System.Windows.Forms;

namespace IP21Performance {

    public partial class Main : Form {

        public Main() {
            InitializeComponent();
            this.working(false);
        }

        private void buttonAdoNetDataAdapter_Click(object sender, EventArgs e) {
            this.go(new AdoNetDataAdapter());
        }

        private void working(Boolean working) {
            this.labelWorking.Visible = working;
            this.Refresh();
        }

        private void go(Method method) {
            this.working(true);

            try {
                string output =
                    method.Go(this.textBoxHostname.Text, this.textBoxPort.Text, this.textBoxUsername.Text, this.textBoxPassword.Text,
                    this.textBoxTag.Text, DateTime.Parse(this.textBoxStartTime.Text), DateTime.Parse(this.textBoxEndTime.Text));

                this.textBoxOutput.Text = output.Replace("\n", "\r\n");
            } catch (Exception e) {
                MessageBox.Show(e.ToString());
            }

            this.working(false);
        }
    }
}