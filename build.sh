GOROOT=`which go`
GOPATH=$HOME/go
GOBIN=$GOPATH/bin
PATH=$PATH:$GOROOT:$GOPATH:$GOBIN
echo "Generating GO bindings..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      protos/klite.proto

# echo "Generating Python bindings..."
# Generate the python client too
# PY_OUT_DIR="../legfinder/tdproxy"
# mkdir -p "$PY_OUT_DIR"
# PY_OUT_DIR="../"
# This is silly - needed to preserve python's directory structure needs
# mkdir -p protos/legfinder/tdproxy
# cp protos/*.proto protos/legfinder/tdproxy
# python -m grpc_tools.protoc -I./protos  --python_out="$PY_OUT_DIR"  --grpc_python_out="$PY_OUT_DIR"  protos/legfinder/tdproxy/auth.proto protos/legfinder/tdproxy/chains.proto protos/legfinder/tdproxy/streamer.proto protos/legfinder/tdproxy/tickers.proto
# rm -Rf protos/legfinder
