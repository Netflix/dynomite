# DynomiteDB - Dynomite (server) package

This directory builds the Dynomite `.deb`  package.

> Building the `.deb` package is for package maintainers. If you want to use Dynomite, then it's quicker and easier to install a signed package as shown below.

Installation instructions for downloading an existing Dynomite package can be found at http://www.dynomitedb.com/download.

## Step-by-step instructions

Package maintainers should use the steps below to build the production Dynomite `.deb` package. 

The instructions below use `~/repos` to simplify the instructions for novices. Experienced developers can change the location of the repository, if desired.

- Install Docker

Follow the installation instructions on https://www.docker.com.

- Compile the Dynomite binaries using Docker

```bash
mkdir ~/repos && git clone https://github.com/Netflix/dynomite.git && cd ~/repos/dynomite

docker run -it --rm -v $PWD:/src dynomitedb/build-dynomite
```

- Create the `.deb` package using fpm

```bash
cd ~/repos/dynomite/packaging/ubuntu

fpm-build-deb.sh 
```
