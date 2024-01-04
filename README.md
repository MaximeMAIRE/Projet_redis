----------------------------------------------------------
# Installation

Firstly, install Visual Studio Code: https://code.visualstudio.com/download
Then, install Python 3.10.12: https://www.python.org/downloads/release/python-31012/

In a terminal:
```
pip install notebook
pip install redis
pip instal pandas
pip install numpy
pip install plotly
sudo apt update && sudo apt upgrade -y
sudo apt-get install redis-server
sudo apt-get install redis-cli
```

In case of failures:
```
redis-server --daemonize yes
```

Then, to lauch the program, you can run the .ipynb.
However, before launching another file, open a terminal in the Projet_redis folder and do:
```
rm mental_health_*
```

In case the file mental_health.sqlite messes up, you can recover it in the backup folder.
