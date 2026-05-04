# Statically compiled aarch64 binaries for debugging

## strace
- Version: 7.0
- Host: Ubuntu 24.04

### How to build
```bash
apt install -y gcc-aarch64-linux-gnu libc6-dev-arm64-cross

wget https://github.com/strace/strace/releases/download/v7.0/strace-7.0.tar.xz

tar xfv strace-7.0.tar.xz

cd strace-7.0/

./configure \
  --build=x86_64-pc-linux-gnu \
  --disable-mpers \
  --host=aarch64-linux-gnu \
  CC=aarch64-linux-gnu-gcc \
  LDFLAGS="-static -pthread"

make -j$(nproc) 
```
The compiled binary is located in `./src/`.