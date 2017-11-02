#!/bin/sh

MASTER='host=localhost port=5432 dbname=demo connect_timeout=3'
SLAVE_DIR=~/demo/slave
SLAVE_BOUNCER=~/demo/etc/pgbouncer.ini-slave
TRY_COUNT=3

psql ()
{
    command ~/inst/pg/bin/psql -X -At -n -q -e --set ON_ERROR_STOP=1 "$@"
}

pg_ctl ()
{
    command ~/inst/pg/bin/pg_ctl "$@"
}

pgbouncer ()
{
    command /usr/sbin/pgbouncer -d -R "$@"
}

is_alive ()
{
    echo -n "is alive? "
    psql -c "select 1" "$MASTER"
}

cnt=0
while [ "$cnt" -lt "$TRY_COUNT" ]; do
    if is_alive; then
	cnt=0
    else
	cnt=$(( cnt + 1 ))
	echo "no answer: $cnt, wait for: $TRY_COUNT"
    fi
    sleep 3
done

echo "dead"

echo "switch pgbouncer"

pgbouncer ~/demo/etc/pgbouncer.ini-slave

echo "promote slave"

pg_ctl promote -D "$SLAVE_DIR"

