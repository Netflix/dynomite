#!/bin/sh
# postinst script for dynomitedb-dynomite
#
# see: dh_installdeb(1)

set -e

# summary of how this script can be called:
#        * <postinst> `configure' <most-recently-configured-version>
#        * <old-postinst> `abort-upgrade' <new version>
#        * <conflictor's-postinst> `abort-remove' `in-favour' <package>
#          <new-version>
#        * <postinst> `abort-remove'
#        * <deconfigured's-postinst> `abort-deconfigure' `in-favour'
#          <failed-install-package> <version> `removing'
#          <conflicting-package> <version>
# for details, see http://www.debian.org/doc/debian-policy/ or
# the debian-policy package

USER="dynomite"
GROUP="dynomite"
HOME="/usr/local/dynomitedb/home"

case "$1" in
    configure)
	# TODO: Set permissions
	mkdir -p /usr/local/dynomitedb/home
	chown -R $USER:$GROUP /usr/local/dynomitedb/home
	#chown $USER:$GROUP /var/lib/redis
	#chown $USER:$GROUP /var/log/redis

	update-rc.d dynomite defaults

	# Start Dynomite with a Redis backend by default
	service dynomite start
    ;;

    abort-upgrade|abort-remove|abort-deconfigure)
    ;;

    *)
        echo "postinst called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac

exit 0
