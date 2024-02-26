
yum -y install libevent-devel zlib-devel openssl-devel
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh  --without-qt5
./configure
make
make install
