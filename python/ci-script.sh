cat /etc/os-release
lsb_release -a
cat /etc/issue
uname -a

yum -y update
yum -y groupinstall "Development Tools"

yum -y install libevent-devel zlib-devel openssl-devel
yum -y install pkg-conf
yum -y install flex
git clone https://github.com/apache/thrift.git
cd thrift
cp /usr/share/aclocal/pkg.m4 aclocal
./bootstrap.sh
./configure --without-qt5
make
make install
find /usr -depth -name "*thrift*"
