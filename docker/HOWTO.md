# Build Image

Create Dynomite plus Redis single server image with docker

    # Example: sudo docker build -t [name] .
    $ sudo docker build -t my_dynomite .

# Running A Dynomite Instance

Creating a container running Dynomite and Redis instance inside. To set the name, you can use the -name [name]. If a name is not set, an alphanumeric ID will be obtained.

    $ sudo docker run -name my_dynomite_instance -i -i my_dynomite

To list all containers

    $ sudo docker ps -l

Enjoy!

