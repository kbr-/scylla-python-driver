#!/bin/bash
set -x

python3 -m venv env
source env/bin/activate
pip install click pyaml 'geomet<0.3,>=0.1'
pip install -e ../ --install-option="--no-libev" --install-option="--no-murmur3" --install-option="--no-cython"
python run_test.py $1
