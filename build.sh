GOROOT=`which go`
GOPATH=$HOME/go
GOBIN=$GOPATH/bin
PATH=$PATH:$GOROOT:$GOPATH:$GOBIN
echo "Generating GO bindings..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      protos/pslite.proto

echo "Generating Python bindings..."
PYPKGNAME=pypslite
# Generate the python client too
git submodule update --init
PY_OUT_DIR="./protos/$PYPKGNAME"
mkdir -p $PY_OUT_DIR/src/$PYPKGNAME
# This is silly - needed to preserve python's directory structure needs
cp protos/*.proto protos/$PYPKGNAME
python3 -m grpc_tools.protoc -I./protos   \
      --python_out="$PY_OUT_DIR"          \
      --grpc_python_out="$PY_OUT_DIR"     \
    protos/$PYPKGNAME/pslite.proto
mv protos/$PYPKGNAME/$PYPKGNAME/* $PY_OUT_DIR/src/$PYPKGNAME
touch $PY_OUT_DIR/src/$PYPKGNAME/__init__.py
# Cleanup files
rm protos/$PYPKGNAME/*.proto
rm -Rf protos/$PYPKGNAME/$PYPKGNAME/
