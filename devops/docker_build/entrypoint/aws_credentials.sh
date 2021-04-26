#!/usr/bin/env bash
#
# If ~/.aws exists loads the AWS credentials into the corresponding environment
# variables.
# 

set -e

# TODO(gp): Load also the AWS_DEFAULT_REGION.

AWS_VOLUME="${HOME}/.aws"
if [[ ! -d $AWS_VOLUME ]]; then
    echo "Can't find $AWS_VOLUME: exiting"
else
    INI_FILE=$AWS_VOLUME/credentials
    while IFS=' = ' read key value
    do
        if [[ $key == \[*] ]]; then
            section=$key
        elif [[ $value ]] && [[ $section == '[default]' ]]; then
            if [[ $key == 'aws_access_key_id' ]]; then
                AWS_ACCESS_KEY_ID=$value
            elif [[ $key == 'aws_secret_access_key' ]]; then
                AWS_SECRET_ACCESS_KEY=$value
            fi
        fi
    done < $INI_FILE
    echo $AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY > /etc/passwd-s3fs-default00-bucket
    chmod 600 /etc/passwd-s3fs-default00-bucket
fi;
