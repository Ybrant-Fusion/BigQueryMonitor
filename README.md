BigQuery Monitor
===============

[Google BigQuery](https://developers.google.com/bigquery/) is a web service that lets you do interactive analysis of massive datasets—up to billions of rows. **BigQuery Monitor** is a jobs monitoring machine written in Python that monitors your Google BigQuery load jobs and sends you an email alert when one or more detected as failed.

## Installation

1. Clone the repository using the clone URL, for example: 
    
    `git clone git@github.com:Ybrant-Fusion/BigQueryMonitor.git`

2. Install Python Package Index (pip) using yum or apt-get (or any other package manager), for example:
    
    `yum install python-pip`

3. Install dependencies using yum or apt-get, for example: 

    `yum install openssl-devel libffi-devel`

4. Install Python dependencies using pip: 

    `pip install google-api-python-client httplib2 pyOpenSSL cryptography python-six cffi pycparser PrettyTable`

5. Edit the configuration file `conf/bquery.conf` to include an appropriate Google BigQuery cerdentials.
   
## Usage

Command line syntax: `bqmonitor [-h] [-v] [--duration DURATION] [--unit UNIT]`

###### Arguments

`-h` : show an help message and exit.

`-v` : show version and exit.

`--duration` : time-frame to monitor (number of days/hours/minutes, depends on the `--units` parameter).

`--unit` : unit of time (`D` for days, `H` for hours or `M` for minutes).
