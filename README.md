# Setup
```powershell
# Create virtual environment
python3 -m venv <venv DIR>

# Install poetry
<venv DIR>\Scripts\python3 -m pip install poetry

# Install dependencies
<venv DIR>\Scripts\poetry install
```

1. Download the [dataset](https://www.kaggle.com/datasets/nolanbconaway/pitchfork-data/data), and store at `./dataset/pitchfork_reviews.sqlite`
2. Create database config at `./db_config.toml`, refer below for the format
```toml
db_user = 'mysql database user name'
db_password = 'mysql database password'
db_endpoint = 'mysql database hostname or endpoint'
db_port = 'mysql database port'
db_name = 'mysql database name'
sqlite_db_relpath = 'sqlite dataset relative pathname'
```

# (Optional) Import dataset to aws ec2 mysql database
## Setup of AWS EC2
- Select ubuntu as OS
- Select free tier in the choices
- Setup atleast 10 gb of EBS volume (free tier)
## Setup of mysql
- Connect to the EC2 instance, then do the following below:
```bash
sudo apt update
sudo apt upgrade

sudo apt-get install mysql-server

# check status
systemctl status mysql

# setup root user
sudo mysql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '[password]';
QUIT;

# Configure MySQl for remote access
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
# change to bind-address = 0.0.0.0

# restart
sudo service mysql restart

# check status again
systemctl status mysql

# check if changes applied, and also check the port
sudo ss -tap | grep mysql

# create user
mysql -u root -p
CREATE USER 'your_user'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON *.* TO 'your_user'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EXIT;

# create database
mysql -u your_user -p
CREATE DATABASE databasename;
EXIT; 

# show port
mysql -u your_user -p
SHOW GLOBAL VARIABLES LIKE 'PORT';
EXIT; 
```