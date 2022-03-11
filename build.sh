GOROOT=`which go`
GOPATH=$HOME/go
GOBIN=$GOPATH/bin
PATH=$PATH:$GOROOT:$GOPATH:$GOBIN
echo "Generating GO bindings..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      protos/pslite.proto

echo "Generating Python bindings..."
# Generate the python client too
# PY_OUT_DIR="../legfinder/tdproxy"
PY_OUT_DIR="./gen/pypslite"
mkdir -p "$PY_OUT_DIR"
# This is silly - needed to preserve python's directory structure needs
# mkdir -p protos/legfinder/tdproxy
# cp protos/*.proto protos/legfinder/tdproxy
python -m grpc_tools.protoc -I./protos  --python_out="$PY_OUT_DIR"  --grpc_python_out="$PY_OUT_DIR"  protos/pslite.proto
# rm -Rf protos/legfinder
