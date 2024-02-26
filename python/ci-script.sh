
yum -y install libevent-devel zlib-devel openssl-devel
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
cat configure
./configure --without-qt5
make
make install
