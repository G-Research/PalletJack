cat /etc/os-release
uname -a

yum -y groupinstall "Development Tools"
yum -y install flex
yum -y install boost boost-thread boost-devel
yum -y install libevent-devel zlib-devel openssl-devel 
git clone https://github.com/apache/thrift.git
cd thrift
cp /usr/share/aclocal/pkg.m4 aclocal
./bootstrap.sh
./configure
make
make install
find /usr -depth -name "*thrift*"
