

DIR=$(pwd)
VENV_DIR=$DIR/.virtualenvs

TARGET_NAME=$1
TAP_NAME=$2
CONFIG_DIR=$DIR/config
TARGET_DIR=$CONFIG_DIR/$TARGET_NAME
TAP_DIR=$TARGET_DIR/$TAP_NAME
LOG_DIR=$TAP_DIR/log
LOG=$LOG_DIR/singer_`date '+%Y%m%d_%H%M%S'`.log

if [[ -z "$TARGET_NAME" || -z "$TAP_NAME" ]]; then
    echo "usage: start-singer TARGET_NAME TAP_NAME"
    exit 1
fi
if [ ! -d "$TARGET_DIR" ]; then
    echo "Config directory not found for target [$TARGET_NAME] at $CONFIG_DIR"
    exit 1
fi

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p $LOG_DIR
fi
exit 1
# Detect tap and target connector
TAP_NAME=`ls $FLOW_DIR | grep tap- | head -n1`
TARGET_NAME=`ls $FLOW_DIR | grep target- | head -n1`

# Derive tap parameters
TAP=$VENV_DIR/$TAP_NAME/bin/$TAP_NAME
TARGET=$VENV_DIR/$TARGET_NAME/bin/$TARGET_NAME
TAP_CONFIG=$TAP_DIR/config.json
TAP_PROPERTIES=$TAP_DIR/properties.json
TAP_STATE=$TAP_DIR/state.json

# Check tap config
if [ ! -d "$TAP_DIR" ]; then
    echo "Config directory not found for tap [$TAP_NAME] at $TAP_CFG_DIR"
    exit 1
fi

#echo $TAP --config $TAP_CONFIG --catalog $TAP_PROPERTIES --state $TAP_STATE
echo $TAP --config $TAP_CONFIG --catalog $TAP_PROPERTIES

#if [ -f "$TAP_STATE" ] && [ `stat -f%z $TAP_STATE` -gt 5 ]; then
#    NEW_STATE=`($TAP --config $TAP_CONFIG --catalog $TAP_PROPERTIES --state $TAP_STATE | $TARGET | tail -n1 ) 2> $LOG`
#else
#    NEW_STATE=`($TAP --config $TAP_CONFIG --catalog $TAP_PROPERTIES | $TARGET | tail -n1 ) 2> $LOG`
#fi

#echo $NEW_STATE > $TAP_STATE
