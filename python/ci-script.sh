
yum -y install libevent-devel zlib-devel openssl-devel pkgconf
yum -y install pkg-conf
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
./configure --without-qt5
make
make install
