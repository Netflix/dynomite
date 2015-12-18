# florida.js

`florida.js` is an http server that returns a list of seed nodes via a REST API.

## Command

```bash
node florida.js [file] [debug]
```

**file** must be a path to a `seeds.list` file. The default `seeds.list` file is `/etc/dynomite/seeds.list`.

**debug** is written as the string "debug" (without the quotes). 

## Run in debug mode

You can run `florida.js` in debug mode (i.e. with messages logged to the console) with the following command.

```bash
cd scripts/Florida
 
npm run debug
```

## Run with a custom seeds file

```bash
cd scripts/Florida

node florida.js ./seeds.list
```

## Run with a custom seeds file in debug mode

```bash
cd scripts/Florida

node florida.js ./seeds.list debug
```
