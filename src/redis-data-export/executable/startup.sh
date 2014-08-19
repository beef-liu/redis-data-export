#!/bin/sh

CURRENT_DIR=${PWD}
#cd "$CURRENT_DIR"
BASEDIR=$CURRENT_DIR
. $BASEDIR/setclasspath.sh

echo JAVA_HOME="$JAVA_HOME"

TMPDIR=$BASEDIR/temp

MAINCLASS=com.MagicCard.CreditCardMate.app.billparser.Bootstrap
CMD_LINE_ARGS=
ACTION=start
SECURITY_POLICY_FILE=
DEBUG_OPTS=
JPDA=
JAVA_OPTS=-Xms128m -Xmx256m -d64

"$JAVA_HOME/bin/jar" -cvfM config.jar -C "$BASEDIR/conf" log4j.properties
mv "$BASEDIR/config.jar" "$BASEDIR/lib/"

CLASSPATH=$CLASSPATH:$BASEDIR/lib/config.jar:$BASEDIR/lib/log4j-1.2.16.jar:$BASEDIR/lib/CollectionCommon.jar:$BASEDIR/lib/jigsaw.jar:$BASEDIR/lib/commons-net-3.2.jar:$BASEDIR/lib/commons-pool-1.6.jar:$BASEDIR/lib/commons-dbcp-1.4.jar:$BASEDIR/lib/javax.mail.jar:$BASEDIR/lib/commons-email-1.3.jar:$BASEDIR/lib/jsoup-1.7.2.jar:$BASEDIR/lib/MetoXML.jar:$BASEDIR/lib/MFSUtil.jar:"$BASEDIR/lib/mysql jdbc5.jar":$BASEDIR/lib/BillParser.jar:$BASEDIR/lib/jackson-core-2.1.4.jar:$BASEDIR/lib/jackson-annotations-2.1.4.jar:$BASEDIR/lib/jackson-databind-2.1.4.jar:$BASEDIR/lib/java-apns.jar:$BASEDIR/lib/CreditCardMateServerNotificationUtil.jar:$BASEDIR/lib/CreditCardMateServerUtil.jar:$BASEDIR/lib/BillParserMainService.jar:
_EXECJAVA=$_RUNJAVA

$_EXECJAVA $JAVA_OPTS $DEBUG_OPTS -classpath "$CLASSPATH" -Djava.io.tmpdir="$TMPDIR" $MAINCLASS $CMD_LINE_ARGS $ACTION &
