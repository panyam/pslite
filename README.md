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
sh build.sh
```
