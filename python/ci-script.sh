
yum -y install libevent-devel zlib-devel openssl-devel pkgconf
yum -y install pkg-conf
git clone https://github.com/apache/thrift.git
cd thrift
cp /usr/share/aclocal/pkg.m4 aclocal
./bootstrap.sh
./configure --without-qt5
make
make install
