A very simple local version of pubsub

## Dev Setup

### Install golang

OSX:

```
brew install golang
```

### Install python (for python bindings)

#### Create and active virtual env

```
python3 -m venv env
source env/bin/activate
```

#### Install Requirements

```
pip install -r dev_requirements
```

### Generate bindings

```
# Needed to get the pypslite package to generate files into
# git submodule init
git submodule update --init
sh build.sh
```
