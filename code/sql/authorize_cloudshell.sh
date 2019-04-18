#!/bin/bash
gcloud sql instances patch testddd \
    --authorized-networks `wget -qO - http://ipecho.net/plain`/32
