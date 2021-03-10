#!/bin/bash -x 

sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl
curl https://pyenv.run | bash
echo "
eval \"$(pyenv init -)\" " >> ~/.bashrc
source ~/.bashrc 


/home/vhuang/.pyenv/versions/3.9.2/bin/python3.9 -m pip install --upgrade pip

which pyenv
pyenv install 3.9.2
pyenv local 3.9.2
pyenv versions
which python 
pip install pyspark


