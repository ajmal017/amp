#!/bin/bash -e

EXEC="/Users/saggese/src/github/github-label-maker/github-label-maker.py"

SRC_DIR="./dev_scripts/github/labels"
DST_DIR="$SRC_DIR/backup"
#gh_tech_labels.json


function label() {
    FULL_OPTS="$OPTS -o $OWNER -r $REPO"
    CMD="python $EXEC $FULL_OPTS -t $GH_TOKEN"
    echo "> $CMD"
    eval $CMD
    echo "Done"
}


# Backup.
if [[ 0 == 1 ]]; then
OWNER="ParticleDev"
REPO="commodity_research"
FILE_NAME="$DST_DIR/labels.$OWNER.$REPO.json"
OPTS="-d $FILE_NAME"
label

FILE_NAME="$SRC_DIR/gh_tech_labels.json"
OPTS="-m $FILE_NAME"
label
fi;

OWNER="alphamatic"
REPO="amp"
FILE_NAME="$DST_DIR/labels.$OWNER.$REPO.json"
OPTS="-d $FILE_NAME"
label

FILE_NAME="$SRC_DIR/gh_tech_labels.json"
OPTS="-m $FILE_NAME"
label
