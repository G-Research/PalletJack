
yum -y install libevent-devel zlib-devel openssl-devel
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
cat configure
./configure --with-qt5=no
make
make install
