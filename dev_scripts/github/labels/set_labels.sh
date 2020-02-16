#!/bin/bash -xe

EXEC="/Users/saggese/src/github/github-label-maker/github-label-maker.py"

SRC_DIR="dev_scripts/github/labels"
DST_DIR="$LABEL_DIR/backup"
#gh_tech_labels.json

# Backup.
OWNER="ParticleDev"
REPO="commodity_research"
FILE_NAME="$DST_DIR/labels.$OWNER.$REPO.json"
OPTS="-d $FILE_NAME -o $OWNER -r $REPO"
CMD="python $EXEC $OPTS  -t $GH_TOKEN"
echo "> $CMD"

OWNER="alphamatic"
REPO="amp"
FILE_NAME="$DST_DIR/labels.$OWNER.$REPO.json"
OPTS="-d $FILE_NAME -o $OWNER -r $REPO"
CMD="python $EXEC $OPTS  -t $GH_TOKEN"
echo "> $CMD"
