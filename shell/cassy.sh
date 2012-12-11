#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: $0 -w [sec]" 1>&2
	echo "   -w, wait interval in seconds " 1>&2
    exit 1
fi

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh \
                   `dirname $0`/cassandra.in.sh; do
        if [ -r $include ]; then
            . $include
            break
        fi
    done
elif [ -r $CASSANDRA_INCLUDE ]; then
    . $CASSANDRA_INCLUDE
fi

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -x $JAVA_HOME/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi

if [ -z $CASSANDRA_CONF -o -z $CLASSPATH ]; then
    echo "You must set the CASSANDRA_CONF and CLASSPATH vars" >&2
    exit 1
fi

for STATS in ring info tpstats cfstats; do     
	#check pid file for process
	if [ -a $STATS.PID ]; then
		c=$(ps -p $(cat $STATS.PID) | wc -l)
		if [ $c -eq 2 ]; then
			echo "$STATS already running with PID: $(cat $STATS.PID)" 1>&2
		else
			echo "$STATS with PID: $(cat $STATS.PID) not in ps list - removing $STATS.PID file" 1>&2
			/bin/rm $STATS.PID >/dev/null 2>&1
		fi
	else
		eval exec $JAVA -cp .:$CLASSPATH:cassy.jar\
			  -Dlog4j.configuration=log4j.${STATS}.properties \
			  com.omnifone.CassyCmd $STATS $@ & >/dev/null 2>&1
		echo $! > $STATS.PID
		echo "$STATS started with PID: $(cat $STATS.PID)"
	fi
done