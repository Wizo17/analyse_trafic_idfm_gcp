#!/bin/bash

gsutil cp gs://analytics_trafic_idfm/source_code/requirements.txt /tmp/requirements.txt

pip install -r /tmp/requirements.txt

