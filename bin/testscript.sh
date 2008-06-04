#!/bin/bash
#$ -S /bin/bash

echo "Sleeping at $(date)"
sleep $1
echo "Waking up at $(date)"

