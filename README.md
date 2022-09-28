# GoogleTrends
The following documentation help you to clone GoogleTrends project in your local system and create a airflow docker container. 

### Create a directory in your local system and get inside the newly created directory

```bash
mkdir HD
cd HD
```

### To Clone the GoogleTrends, we need to initialise the git inside the "HD" directory
```bash
git init
git config --global init.defaultBranch master
git clone https://github.com/Svkworks/GoogleTrends.git
```

### Once you clone the GoogleTrends project, then move to "GoogleTrends/docker" directory using following command

```bash
cd GoogleTrends/docker 
```

### Please add your google_auth.json file in following location 

``` bash 
cd mnt/airflow/utils
ls -l
total 24
-rw-r--r--  1 saim  staff   182 28 Sep 19:24 config.yaml
-rw-r--r--@ 1 saim  staff  2341 26 Sep 21:06 google_auth.json 
-rw-r--r--  1 saim  staff   306 28 Sep 19:24 test.py
```

### Once you add google_auth.json, Please run the following commands to go to "docker" directory

``` bash
pwd
/Users/saim/Documents/HD/GoogleTrends/docker/mnt/airflow/utils

cd ../../../

pwd
/Users/saim/Documents/HD/GoogleTrends/docker
```

###  Following command will create airflow docker,  (Note: Your docker folder should be your present working directory (PWD)) 

``` bash
/bin/bash start.sh 
```
> Once you run the above command, it will create a snowflake docker container, to verify the container status please go to "http://localhost:8080" 
> You can see airflow login page, and please use the following credentials to access the airflow webUI 

```
username : airflow
password : airflow
```
