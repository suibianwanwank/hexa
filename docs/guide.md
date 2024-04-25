# Guide
This tutorial covers:
* Start and use the three components with a docker container
* How developers compile and run project
* A simple example to register data and complete a query
## Docker
>docker pull


## Compiling projects
Fe, Be, and Cli are three separate components connected by grpc communication, so you can compile or start a project individually

Now let's download the program
>git clone https://github.com/suibianwanwank/hexa.git
#### BackEnd
backend is a rust project, First make sure you have the rust environment installed
> cd ./be
> 
> cargo build
>
After completing the compilation, Launching of the project
> cd target/debug
> 
> ./hexa-be

You can check out --help for more startup configurations

#### Cli
Same rust project, similar to BE approach

#### FrontEnd
Frontend is a java project, again, you need to make sure you have java8 and maven installed.
> cd ./fe
>
> mvn clean install

After the compilation is complete, you need to add the vm option to specify the path to the configuration file
> -Dconfig=fe/main/src/main/resources/config.properties

The default configuration path is as above,In addition, the rocksdb storage code is now deprecated, 
you need to specify the url and database of mongodb as the project datastore in the configuration file

> datastore.mongodb.uri = mongodb://username:password@host:port/?authSource=admin&
> 
> datastore.mongodb.db = hexa
