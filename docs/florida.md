# Florida

## Florida Multi-Cluster Solution

Let's say you want to have multiple clusters so you need to have different Florida URLs providing different seeds for each cluster. In such a case, the following environment variables can be used to customize the behavior of Dynomite without requiring a custom build (via Make).

- `DYNOMITE_FLORIDA_IP`
- `DYNOMITE_FLORIDA_PORT`
- `DYNOMITE_FLORIDA_REQUEST`

The Florida server only reads the environment variables one time when the server is started. If the environment variables are not present (NULL) the code will use the macros or CFLAGS values. This is great because it allows us to use the same binary all the time, and then later when I deploy the code into ASGARD I can have just different user data since is just a matter of export different values to the AMI/Linux. There is a minor FIX of YAML syntax as well.

So for instance someone can have in the `/etc/profile`

```bash
export DYNOMITE_FLORIDA_PORT=8080
export DYNOMITE_FLORIDA_IP="127.0.0.1"
export DYNOMITE_FLORIDA_REQUEST="GET /florida/cluster1/get_seeds.txt HTTP/1.0
Host: 127.0.0.1
User-Agent: HTMLGET 1.0

"
```

and for another box just

```bash
export DYNOMITE_FLORIDA_PORT=8080
export DYNOMITE_FLORIDA_IP="127.0.0.1"
export DYNOMITE_FLORIDA_REQUEST="GET /florida/cluster2/get_seeds.txt HTTP/1.0
Host: 127.0.0.1
User-Agent: HTMLGET 1.0

"
```

Even pass this through ASGARD via user_data.

Then build dynomite normally like:

```bash
sudo autoreconf -fvi ; sudo ./configure --enable-debug=log ; sudo make;
```

And Run:

```bash
sudo --preserve-env src/dynomite -c conf/dynomite_florida_single.yml
```

And it works :-)

## florida.js

`florida.js` is an http server that returns a list of seed nodes via a REST API.

### Command

```bash
node florida.js [file] [debug]
```

**file** must be a path to a `seeds.list` file. The default `seeds.list` file is `/etc/dynomite/seeds.list`.

**debug** is written as the string "debug" (without the quotes). 

### Run in debug mode

You can run `florida.js` in debug mode (i.e. with messages logged to the console) with the following command.

```bash
cd scripts/Florida
 
npm run debug
```

### Run with a custom seeds file

```bash
cd scripts/Florida

node florida.js ./seeds.list
```

### Run with a custom seeds file in debug mode

```bash
cd scripts/Florida

node florida.js ./seeds.list debug
```
