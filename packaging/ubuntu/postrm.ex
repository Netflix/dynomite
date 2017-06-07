#!/bin/sh
# postrm script for dynomite
#
# see: dh_installdeb(1)

set -e

# summary of how this script can be called:
#        * <postrm> `remove'
#        * <postrm> `purge'
#        * <old-postrm> `upgrade' <new-version>
#        * <new-postrm> `failed-upgrade' <old-version>
#        * <new-postrm> `abort-install'
#        * <new-postrm> `abort-install' <old-version>
#        * <new-postrm> `abort-upgrade' <old-version>
#        * <disappearer's-postrm> `disappear' <overwriter>
#          <overwriter-version>
# for details, see http://www.debian.org/doc/debian-policy/ or
# the debian-policy package


case "$1" in
    purge|remove|upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
        # Delete dynomite user if user exists
        if getent passwd dynomite >/dev/null
        then
            userdel dynomite >/dev/null
        fi

	#
	# Group must be deleted AFTER use
	#

	# Delete dynomite group if group exists        
	if getent group dynomite >/dev/null
        then
	    groupdel dynomite >/dev/null
        fi

	update-rc.d -f dynomite remove
    ;;

    *)
        echo "postrm called with unknown argument \`$1'" >&2
        exit 1
    ;;
esac

exit 0
