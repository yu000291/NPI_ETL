# !/bin/bash
cd /home/datainsights/gsc_npi_load/
source /home/datainsights/miniconda3/bin/activate npi_load
# export LD_LIBRARY_PATH=/opt/oracle/instantclient_18_3:$LD_LIBRARY_PATH
export JAVA_HOME=/usr/bin/java
python="/home/datainsights/miniconda3/envs/npi_load/bin/python"
nohup ${python} npi_load.py >> /home/datainsights/gsc_npi_load/.log/npi_load.log 2>&1&
