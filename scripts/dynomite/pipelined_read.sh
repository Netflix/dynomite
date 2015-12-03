#!/bin/sh

#socatopt="-t 4 -T 4 -b 8193 -d -d "
socatopt="-t 8 -T 8 -b 16384 -d -d"

get_commands=""

# build
for i in `seq 1 64`; do
    if [ `expr $i % 2` -eq "0" ]; then
        key="foo"
    else
        key="bar"
    fi
    key=`printf "%s%d" "${key}" "${i}"`
    keylen=`printf "%s" "${key}" | wc -c`

    get_command="*2\r\n\$3\r\nget\r\n\$${keylen}\r\n${key}\r\n"
    get_commands=`printf "%s%s" "${get_commands}" "${get_command}"`
done

printf "%b" "$get_commands" > /tmp/socat.input
# read
for i in `seq 1 64`; do
    cat /tmp/socat.input | socat ${socatopt} - TCP:localhost:8102,nodelay,shut-down,nonblock=1 & #1 > /dev/null 2>&1 &
done
wait
