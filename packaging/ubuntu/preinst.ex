#!/bin/sh
# preinst script for dynomitedb-dynomite
#
# see: dh_installdeb(1)

set -e

# summary of how this script can be called:
#        * <new-preinst> `install'
#        * <new-preinst> `install' <old-version>
#        * <new-preinst> `upgrade' <old-version>
#        * <old-preinst> `abort-upgrade' <new-version>
# for details, see http://www.debian.org/doc/debian-policy/ or
# the debian-policy package

USER="dynomite"
GROUP="dynomite"
HOME="/usr/local/dynomitedb/home"

case "$1" in
    install|upgrade)
	# If NIS is used, then allow errors
	if which ypwhich >/dev/null 2>&1 && ypwhich >/dev/null 2>&1
	then
		set +e
	fi

	# Add dynomite group if group does not exist
	if ! getent group $GROUP >/dev/null
	then
		addgroup --system $GROUP >/dev/null
	fi

	# Add dynomite user if user does not exist
	if ! getent passwd $USER >/dev/null
	then
	    adduser --system --disabled-login --ingroup dynomite --no-create-home --home /usr/local/dynomitedb/home --gecos "dynomitedb" --shell /bin/false dynomite >/dev/null
	fi

	# end of NIS tolerance zone
	set -e

    ;;

    abort-upgrade)
    ;;

    *)
        echo "preinst called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac

exit 0
