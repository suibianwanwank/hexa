# Guide
This tutorial covers:
* Start and use the three components with a docker container
* How developers compile and run project
* A simple example to register data and complete a query
## Docker
Fe
```shell
docker pull suibianwanwan333/hexa-fe:@v1.0.0

#Configuration items can be viewed /fe/etc/config.properties
docker cp CONFIG_FILE/config.properties  CONTAINER_ID:/fe/etc/config.properties

docker run -d -p 9065:9065 suibianwanwan333/hexa-fe:v1.0.0
```

Be
```shell
docker pull suibianwanwan333/hexa-be:@v1.0.0

# mapping port 
docker run -d -p 8888:8888 suibianwanwan333/hexa-be:v1.0.0
```

## Compiling projects
Fe, Be, and Cli are three separate components connected by grpc communication, so you can compile or start a project individually

Now let's download the program
```terminal
git clone https://github.com/suibianwanwank/hexa.git
```
#### BackEnd
backend is a rust project, First make sure you have the rust environment installed
```shell
cd ./be

cargo build
```
After completing the compilation, Launching of the project
```shell
cd target/debug

./hexa-be
```
You can check out --help for more startup configurations

#### Cli
Same rust project, similar to BE approach

#### FrontEnd
Frontend is a java project, again, you need to make sure you have java8 and maven installed.
```shell
cd ./fe

mvn clean install
```

After the compilation is complete, you need to add the vm option to specify the path to the configuration file
```shell
-Dconfig=fe/main/src/main/resources/config.properties
```

The default configuration path is as above,In addition, the rocksdb storage code is now deprecated, 
you need to specify the url and database of mongodb as the project datastore in the configuration file
```shell
datastore.mongodb.uri = mongodb://username:password@host:port/?authSource=admin&

datastore.mongodb.db = hexa
```



## Usage
Create first catalog named 'pg'
```sql
create catalog POSTGRESQL as pg with {sourceType: POSTGRESQL,  host:127.0.0.1, port: 5432, username:root,  password:suibianwanwan, database: postgres};
```
Show create sql with new catalog 'pg'
```sql
show create catalog pg
```
Show metadata
```sql
show schemas from pg
show tables from pg.tpcds
show columns from pg.tpcds.store
```
Execute a simple query
```sql
select * from pg.tpcds.store;
```